"""
apps/api/services/entity_extraction.py

Entity extraction pipeline.

Extraction strategy (ordered by priority):
  1. spaCy NER  — high accuracy for PERSON, ORG, GPE, LOC, DATE.
                  Used automatically when the spaCy model is loadable.
                  Install: pip install spacy && python -m spacy download en_core_web_sm
  2. Regex       — fast, zero deps. Covers domain-specific types that spaCy misses:
                  Aircraft tail numbers, phone numbers, emails, ICAO airport codes.
                  Also fills in when spaCy is not installed.

The two results are merged: spaCy takes precedence for types it covers well;
regex fills the gaps for types it handles exclusively (Aircraft, Phone, Email, Airport, Address).

Deduplication key: (entity_type, canonical_name, char_start, char_end)
Each distinct occurrence at a distinct offset becomes its own Mention row,
so mention counts accurately reflect how often an entity appears in a document.
"""

import re
import logging
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional

log = logging.getLogger(__name__)

try:
    from apps.narrative.intel_engine import is_ocr_junk as _is_ocr_junk
except ImportError:
    _is_ocr_junk = None


@dataclass
class ExtractedEntity:
    entity_type: str       # Person | Organization | Location | Aircraft | Phone | Email | Date | Airport
    raw_name: str          # Text as it appears in the document
    canonical_name: str    # Normalized form used for deduplication
    char_start: int
    char_end: int
    context: str           # ±150 chars surrounding the match
    confidence: float
    extraction_method: str # "spacy" | "regex"


# ── spaCy ──────────────────────────────────────────────────────────────────────

_SPACY_LABEL_MAP = {
    "PERSON": "Person",
    "ORG":    "Organization",
    "GPE":    "Location",
    "LOC":    "Location",
    "DATE":   "Date",
    "EVENT":  "Event",
    "FAC":    "Facility",
    "NORP":   "Organization",  # nationalities, religious/political groups
}

# spaCy entity types that regex should NOT override (spaCy is more accurate)
_SPACY_AUTHORITATIVE = frozenset({"Person", "Organization", "Location", "Date", "Event", "Facility"})



# ── Entity quality filter ──────────────────────────────────────────────────────

# Words that look like capitalized phrases but are never real entities
_NOISE_WORDS = frozenset({
    # Document structure
    "Introduction", "Conclusion", "Summary", "Abstract", "Background",
    "Overview", "Context", "Analysis", "Discussion", "Methodology",
    "Results", "Findings", "Recommendations", "Appendix", "References",
    "Section", "Chapter", "Part", "Figure", "Table", "Exhibit",
    # Legal/report section headers
    "Issues This", "Based Approach", "Healthy Co", "Issues", "Approach",
    "The Risk", "The Potential", "The Challenge",
    # Common phrases
    "However", "Therefore", "Furthermore", "Moreover", "Additionally",
    "Consequently", "Nevertheless", "Alternatively", "Subsequently",
    "Specifically", "Generally", "Typically", "Currently", "Previously",
    # Single words that aren't names  
    "True", "False", "None", "Null",
})

_NOISE_PATTERNS = [
    # Gerund phrases (verb+ing + noun) — never a person name
    re.compile(r'^[A-Z][a-z]+ing\s+[A-Z]', re.IGNORECASE),
    # 3+ word phrases where all words are capitalized common words
    re.compile(r'^([A-Z][a-z]+\s+){3,}[A-Z][a-z]+$'),
    # Starts with "The " 
    re.compile(r'^The\s+[A-Z]'),
    # Section header patterns like "X: Y" 
    re.compile(r'^[A-Z][a-zA-Z\s]+:\s'),
    # All caps abbreviations that aren't aircraft/airports
    re.compile(r'^[A-Z]{5,}$'),
]

def _is_noise_entity(name: str, entity_type: str) -> bool:
    """Return True if this extraction is likely noise (section header, phrase, etc.)"""
    if entity_type not in ("Person", "Organization", "Location", "Event", "Facility"):
        return False
    
    stripped = name.strip()
    
    # Too short or too long
    if len(stripped) < 3 or len(stripped) > 60:
        return True
    
    # In noise word list
    if stripped in _NOISE_WORDS or stripped.rstrip("s") in _NOISE_WORDS:
        return True
    
    # Matches noise patterns
    for pattern in _NOISE_PATTERNS:
        if pattern.search(stripped):
            return True
    
    # For Person: must look like an actual name (not a phrase)
    if entity_type == "Person":
        words = stripped.split()
        # Reject if any word is a common English word that isn't a surname
        _COMMON_WORDS = {
            "summary", "dynamics", "syndrome", "critique", "markers",
            "identifying", "quantifying", "addressing", "rejecting",
            "approaches", "perspectives", "challenges", "responses",
            "impact", "manipulation", "training", "analysis", "hearing",
            "terminology", "protection", "assessment", "evaluation",
            "this", "the", "and", "for", "with", "from", "into",
            "healthy", "harmful", "appropriate", "statistical", "linguistic",
            "multifaceted", "computational", "international", "judicial",
        }
        word_set = {w.lower() for w in words}
        if word_set & _COMMON_WORDS:
            return True
        # Reject if more than 3 words (names are 1-3 words)
        if len(words) > 3:
            return True
    
    return False


@lru_cache(maxsize=4)
def _load_spacy(model_name: str):
    """
    Load a spaCy model, returning None if spaCy or the model is unavailable.
    Result is cached — model loading is expensive.
    """
    try:
        import spacy  # type: ignore
        nlp = spacy.load(model_name)
        log.info("spaCy model '%s' loaded", model_name)
        return nlp
    except ImportError:
        log.info("spaCy not installed — using regex extraction only")
        return None
    except OSError:
        log.warning(
            "spaCy model '%s' not found. Run: python -m spacy download %s",
            model_name, model_name,
        )
        return None


def extract_spacy(
    text: str,
    document_id: str,
    page_number: int,
    model_name: str = "en_core_web_sm",
) -> list[ExtractedEntity]:
    """
    Run spaCy NER on text. Returns [] gracefully if spaCy is unavailable.
    Processes up to 100,000 characters (sufficient for a dense document page).
    """
    nlp = _load_spacy(model_name)
    if nlp is None:
        return []

    doc = nlp(text[:100_000])
    results: list[ExtractedEntity] = []
    seen: set[tuple] = set()

    for ent in doc.ents:
        etype = _SPACY_LABEL_MAP.get(ent.label_)
        if not etype:
            continue
        canonical = _normalize(ent.text, etype)
        if not canonical:
            continue

        key = (etype, canonical, ent.start_char, ent.end_char)
        if key in seen:
            continue
        seen.add(key)

        results.append(ExtractedEntity(
            entity_type=etype,
            raw_name=ent.text,
            canonical_name=canonical,
            char_start=ent.start_char,
            char_end=ent.end_char,
            context=text[max(0, ent.start_char - 150): ent.end_char + 150],
            confidence=0.85,
            extraction_method="spacy",
        ))

    return results


# ── Regex patterns ─────────────────────────────────────────────────────────────

_PATTERNS: list[tuple[str, re.Pattern, float]] = [
    (
        "Aircraft",
        re.compile(r'\b(N\d{1,5}[A-Z]{0,2}|G-[A-Z]{4}|VP-[A-Z]{3}|VH-[A-Z]{3}|C-[A-Z]{4})\b'),
        0.95,
    ),
    (
        "Phone",
        re.compile(
            r'\b(\+?1[-.\s]?)?\(?([2-9]\d{2})\)?[-.\s]?([2-9]\d{2})[-.\s]?(\d{4})\b'
        ),
        0.92,
    ),
    (
        "Email",
        re.compile(r'\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b'),
        0.97,
    ),
    (
        "Address",
        re.compile(
            r'\b\d{1,5}\s+[A-Z][a-z]+(?:\s+[A-Za-z]+){1,4}'
            r'\s+(?:Street|St|Avenue|Ave|Boulevard|Blvd|Road|Rd|Drive|Dr|'
            r'Lane|Ln|Court|Ct|Place|Pl|Way|Circle|Cir)\b',
            re.IGNORECASE,
        ),
        0.82,
    ),
    (
        "Airport",
        re.compile(r'\b(K[A-Z]{3}|[A-Z]{4})\b'),
        0.75,
    ),
    (
        "Date",
        re.compile(
            r'\b(?:'
            r'\d{4}[-/]\d{2}[-/]\d{2}'
            r'|(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?'
            r'|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)'
            r'\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}'
            r'|\d{1,2}/\d{1,2}/\d{2,4}'
            r')\b',
            re.IGNORECASE,
        ),
        0.88,
    ),
    (
        "Person",
        # Heuristic: 2-3 capitalised words. Low confidence; spaCy is preferred.
        re.compile(
            r'(?<![.!?]\s)(?<!\n)'
            r'\b([A-Z][a-z]{1,20}'
            r'(?:\s+(?:van|de|von|der|la|le|del|bin|al|el|st\.?|jr\.?|sr\.?|ii|iii))?'
            r'\s+[A-Z][a-z]{1,20}'
            r'(?:\s+[A-Z][a-z]{1,20})?)\b'
        ),
        0.55,
    ),
]

# Common English words that match the 4-letter ICAO airport pattern — excluded.
_AIRPORT_EXCLUDE: frozenset[str] = frozenset({
    "ALSO", "AREA", "BASE", "BEST", "BLUE", "BODY", "BOLD", "BOND", "BOOK",
    "CALL", "CALM", "CAME", "CARE", "CASE", "CASH", "CAST", "CITY", "CLUB",
    "CODE", "COLD", "COME", "COPY", "CORE", "CORN", "COST", "DATA", "DATE",
    "DAYS", "DEAD", "DEAL", "DEAR", "DEEP", "DENY", "DESK", "DONE", "DOOR",
    "DOWN", "DROP", "DUAL", "DUST", "EACH", "EARN", "EASE", "EAST", "EDIT",
    "EVEN", "EVER", "EVIL", "FACE", "FACT", "FAIL", "FAIR", "FALL", "FAME",
    "FAST", "FATE", "FEEL", "FILE", "FILM", "FIND", "FINE", "FIRE", "FIRM",
    "FISH", "FIVE", "FLAG", "FLAT", "FLOW", "FOAM", "FOLD", "FONT", "FOOD",
    "FORM", "FORT", "FREE", "FROM", "FUEL", "FULL", "FUND", "GAIN", "GAME",
    "GAVE", "GEAR", "GETS", "GIFT", "GIVE", "GLAD", "GOAL", "GOLD", "GONE",
    "GOOD", "GREW", "GROW", "GUNS", "HAIR", "HALF", "HALL", "HAND", "HANG",
    "HARD", "HARM", "HATE", "HAVE", "HEAD", "HEAT", "HELD", "HELP", "HERE",
    "HIGH", "HILL", "HINT", "HIRE", "HOLE", "HOME", "HOPE", "HOST", "HOUR",
    "HUGE", "HUNT", "HURT", "IDEA", "INTO", "IRON", "ITEM", "JAIL", "JOIN",
    "JUMP", "JUST", "KEEN", "KEEP", "KICK", "KIND", "KNEW", "KNOW", "LACK",
    "LAID", "LAKE", "LAND", "LAST", "LATE", "LEAD", "LEAN", "LEFT", "LIFE",
    "LIFT", "LIKE", "LINE", "LINK", "LIST", "LIVE", "LOAD", "LOAN", "LOCK",
    "LONE", "LONG", "LOOK", "LOSE", "LOSS", "LOST", "LOUD", "LOVE", "LUCK",
    "MADE", "MAIL", "MAIN", "MAKE", "MALE", "MANY", "MARK", "MASS", "MEAN",
    "MEET", "MENU", "MERE", "MILD", "MILE", "MILL", "MINE", "MISS", "MODE",
    "MORE", "MOST", "MOVE", "MUCH", "MUST", "NAME", "NAVY", "NEAR", "NEED",
    "NEWS", "NEXT", "NODE", "NONE", "NORM", "NOTE", "NULL", "ONCE", "ONLY",
    "OPEN", "ORAL", "OVER", "PAGE", "PAID", "PAIN", "PAIR", "PARK", "PART",
    "PASS", "PAST", "PATH", "PEAK", "PICK", "PILE", "PIPE", "PLAN", "PLAY",
    "PLOT", "PLUS", "POLL", "POOL", "POOR", "PORT", "POST", "POUR", "PULL",
    "PUMP", "PURE", "PUSH", "RACE", "RAIL", "RAIN", "RANK", "RARE", "RATE",
    "READ", "REAL", "REAR", "REST", "RICE", "RICH", "RIDE", "RING", "RISE",
    "RISK", "ROLE", "ROLL", "ROOF", "ROOM", "ROOT", "ROPE", "ROSE", "RULE",
    "RUSH", "SAFE", "SAID", "SAIL", "SALE", "SALT", "SAME", "SAND", "SAVE",
    "SEAL", "SEAT", "SEED", "SEEK", "SELF", "SELL", "SEND", "SENT", "SHIP",
    "SHOP", "SHOT", "SHOW", "SHUT", "SIDE", "SIGN", "SITE", "SIZE", "SKIN",
    "SLIP", "SLOW", "SNAP", "SNOW", "SOIL", "SOLD", "SOLE", "SOME", "SONG",
    "SOON", "SORT", "SOUL", "SOUP", "SPIN", "SPOT", "STAR", "STAY", "STEM",
    "STEP", "STOP", "SUIT", "TALE", "TALL", "TANK", "TAPE", "TASK", "TEAM",
    "TEAR", "TELL", "TEND", "TERM", "TEST", "TEXT", "THAN", "THAT", "THEM",
    "THEN", "THEY", "THIN", "THIS", "TICK", "TIDE", "TIED", "TILE", "TIME",
    "TINY", "TIRE", "TOLD", "TOLL", "TONE", "TOOK", "TOOL", "TOSS", "TOUR",
    "TOWN", "TREE", "TRIM", "TRUE", "TUBE", "TUNE", "TURN", "TYPE", "UGLY",
    "UNIT", "UPON", "USED", "USER", "VAST", "VERY", "VICE", "VIEW", "VINE",
    "VOID", "VOTE", "WAGE", "WAKE", "WALK", "WALL", "WANT", "WARD", "WARM",
    "WARN", "WASH", "WAVE", "WAYS", "WEAK", "WEAR", "WEEK", "WELL", "WENT",
    "WERE", "WHAT", "WHEN", "WHOM", "WIDE", "WIFE", "WILD", "WILL", "WIND",
    "WINE", "WING", "WIRE", "WISE", "WISH", "WITH", "WOOD", "WORD", "WORK",
    "WORN", "WRAP", "YARD", "YEAR", "YOUR", "ZERO", "ZONE",
    # FOIA / legal
    "FOIA", "NOTE", "FILE", "CASE", "FORM", "DEPT", "SUBJ", "FROM", "SENT",
    "THRU", "ATTN", "ENCL", "REFS", "PARA", "SECT", "PAGE", "COPY",
})


def _normalize(raw: str, entity_type: str) -> str:
    """Return a stable canonical form for deduplication and display."""
    text = re.sub(r'\s+', ' ', raw).strip()
    if entity_type in ("Aircraft", "Airport"):
        return text.upper()
    if entity_type == "Phone":
        return re.sub(r'[^\d+]', '', text)
    if entity_type == "Email":
        return text.lower()
    if entity_type == "Person":
        text = re.sub(r',?\s+(?:Jr\.?|Sr\.?|II|III|IV|Esq\.?)$', '', text, flags=re.I)
        return text.title()
    return text


def extract_regex(
    text: str,
    document_id: str,
    page_number: int,
) -> list[ExtractedEntity]:
    """
    Run all regex patterns. Dedup key is (entity_type, canonical_name, char_start, char_end)
    so each distinct occurrence at a distinct position produces a separate result.
    """
    results: list[ExtractedEntity] = []
    seen: set[tuple] = set()

    for entity_type, pattern, confidence in _PATTERNS:
        for match in pattern.finditer(text):
            raw = match.group().strip()
            if not raw:
                continue

            if entity_type == "Airport" and raw.upper() in _AIRPORT_EXCLUDE:
                continue

            canonical = _normalize(raw, entity_type)
            if not canonical:
                continue

            start, end = match.start(), match.end()
            key = (entity_type, canonical, start, end)
            if key in seen:
                continue
            seen.add(key)

            results.append(ExtractedEntity(
                entity_type=entity_type,
                raw_name=raw,
                canonical_name=canonical,
                char_start=start,
                char_end=end,
                context=text[max(0, start - 150): end + 150],
                confidence=confidence,
                extraction_method="regex",
            ))

    return results


def extract_entities(
    text: str,
    document_id: str,
    page_number: int,
    spacy_model: str = "en_core_web_sm",
) -> list[ExtractedEntity]:
    """
    Primary entry point. Returns one ExtractedEntity per distinct occurrence.

    Strategy:
      1. Attempt spaCy NER. If the model is available, use it as the primary
         source for PERSON, ORG, GPE, DATE and related types.
      2. Run regex unconditionally for domain-specific types (Aircraft, Phone,
         Email, Airport, Address) that spaCy does not extract reliably.
      3. If spaCy was unavailable, use regex results for all types (including
         Person/Org at lower confidence).

    This means the system degrades gracefully: it still extracts entities
    without spaCy, and uses spaCy automatically when it is installed.
    """
    if not text.strip():
        return []

    # Skip pages that are pure OCR garbage (FOIA redactions, numeric tables)
    if _is_ocr_junk is not None and _is_ocr_junk(text[:2000], "foia"):
        log.debug("[extract_entities] OCR junk detected, skipping page")
        return []

    spacy_results = extract_spacy(text, document_id, page_number, spacy_model)
    regex_results = extract_regex(text, document_id, page_number)

    if not spacy_results:
        # spaCy unavailable or produced nothing — return full regex set.
        return [e for e in regex_results if not _is_noise_entity(e.canonical_name, e.entity_type)]

    # spaCy succeeded: use it for types it covers authoritatively.
    # Keep regex results only for types not covered by spaCy.
    spacy_positions: set[tuple[int, int]] = {
        (e.char_start, e.char_end) for e in spacy_results
    }

    # Filter noise from spacy results
    spacy_results = [e for e in spacy_results if not _is_noise_entity(e.canonical_name, e.entity_type)]
    merged: list[ExtractedEntity] = list(spacy_results)
    for r in regex_results:
        if r.entity_type in _SPACY_AUTHORITATIVE:
            # Regex result for a spaCy-covered type: include only if spaCy
            # did not already produce a result at this offset.
            if (r.char_start, r.char_end) not in spacy_positions:
                merged.append(r)
        else:
            # Regex-exclusive type (Aircraft, Phone, Email, Airport, Address).
            merged.append(r)

    return merged
