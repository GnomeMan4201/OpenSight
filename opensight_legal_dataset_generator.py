#!/usr/bin/env python3
"""
opensight_legal_dataset_generator.py

Generate hundreds or thousands of synthetic legal documents for OpenSight testing.

What it creates:
- Multiple legal case clusters
- Repeated entities across documents
- Timelines over multiple years
- Relationship edges for graph construction
- JSON documents compatible with a typical OpenSight-style ingest pipeline

Output structure:
examples/opensight_synthetic_legal_dataset/
├── metadata.json
└── documents/
    ├── doc_00001.json
    ├── doc_00002.json
    └── ...

Usage:
    python opensight_legal_dataset_generator.py
    python opensight_legal_dataset_generator.py --count 500
    python opensight_legal_dataset_generator.py --count 750 --seed 42
    python opensight_legal_dataset_generator.py --out ./examples/legal_demo

Notes:
- This generates synthetic test data, not real court records.
- Adjust field names if your OpenSight ingest expects a different schema.
"""

from __future__ import annotations

import argparse
import json
import random
import re
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Tuple


# ----------------------------
# Utilities
# ----------------------------

def slugify(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def daterange(start: date, end: date) -> List[date]:
    days = (end - start).days
    return [start + timedelta(days=i) for i in range(days + 1)]


def choose_date(start: date, end: date) -> date:
    options = daterange(start, end)
    return random.choice(options)


def json_write(path: Path, data: Dict) -> None:
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


# ----------------------------
# Domain Models
# ----------------------------

@dataclass
class Relationship:
    source: str
    type: str
    target: str

    def as_dict(self) -> Dict[str, str]:
        return {
            "source": self.source,
            "type": self.type,
            "target": self.target,
        }


@dataclass
class CaseTemplate:
    cluster_name: str
    case_name: str
    court: str
    court_type: str
    start_date: date
    end_date: date
    parties: List[str]
    judges: List[str]
    agencies: List[str]
    topics: List[str]
    event_flow: List[str]
    bridge_entities: List[str] = field(default_factory=list)

    def all_core_entities(self) -> List[str]:
        entities = [self.case_name, self.court]
        entities.extend(self.parties)
        entities.extend(self.judges)
        entities.extend(self.agencies)
        entities.extend(self.bridge_entities)
        return dedupe(entities)


# ----------------------------
# Helpers
# ----------------------------

def dedupe(values: List[str]) -> List[str]:
    seen = set()
    out = []
    for v in values:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def maybe(prob: float) -> bool:
    return random.random() < prob


def pick_some(values: List[str], min_n: int = 1, max_n: int | None = None) -> List[str]:
    if not values:
        return []
    if max_n is None:
        max_n = len(values)
    max_n = min(max_n, len(values))
    min_n = min(min_n, max_n)
    n = random.randint(min_n, max_n)
    return random.sample(values, n)


# ----------------------------
# Synthetic Vocabulary
# ----------------------------

EVENT_TEMPLATES: Dict[str, Dict] = {
    "complaint_filed": {
        "title": "Complaint Filed",
        "summary_templates": [
            "{plaintiff} filed a complaint against {defendant} in {court}, alleging {topic}.",
            "A civil complaint was entered in {court} naming {defendant} as defendant and alleging {topic}.",
        ],
        "relationships": [
            ("{plaintiff}", "filed_complaint_against", "{defendant}"),
            ("{court}", "docketed", "{case_name}"),
        ],
        "entity_extras": [],
    },
    "motion_to_dismiss": {
        "title": "Motion to Dismiss",
        "summary_templates": [
            "{defendant} filed a motion to dismiss, arguing the complaint failed to establish {topic}.",
            "Defense counsel for {defendant} moved to dismiss portions of the complaint involving {topic}.",
        ],
        "relationships": [
            ("{defendant}", "filed_motion", "{case_name}"),
            ("{judge}", "reviewing_motion_in", "{case_name}"),
        ],
        "entity_extras": [],
    },
    "discovery_order": {
        "title": "Discovery Order",
        "summary_templates": [
            "{judge} issued a discovery order requiring production of records related to {topic}.",
            "The court ordered limited discovery focused on communications and materials tied to {topic}.",
        ],
        "relationships": [
            ("{judge}", "issued_order_in", "{case_name}"),
            ("{court}", "ordered_discovery_in", "{case_name}"),
        ],
        "entity_extras": ["Document Production", "Internal Emails", "Financial Records"],
    },
    "hearing": {
        "title": "Hearing Held",
        "summary_templates": [
            "The court held a hearing in {case_name} to address disputed issues concerning {topic}.",
            "{judge} conducted a hearing involving arguments by {plaintiff} and {defendant} on {topic}.",
        ],
        "relationships": [
            ("{judge}", "held_hearing_for", "{case_name}"),
            ("{plaintiff}", "appeared_in", "{case_name}"),
            ("{defendant}", "appeared_in", "{case_name}"),
        ],
        "entity_extras": ["Court Hearing", "Oral Argument"],
    },
    "expert_report": {
        "title": "Expert Report Submitted",
        "summary_templates": [
            "An expert report was submitted addressing technical issues related to {topic}.",
            "The parties exchanged expert materials concerning damages, causation, and {topic}.",
        ],
        "relationships": [
            ("Expert Witness", "submitted_report_in", "{case_name}"),
            ("{case_name}", "includes_issue", "{topic}"),
        ],
        "entity_extras": ["Expert Witness", "Damages Model"],
    },
    "settlement_talks": {
        "title": "Settlement Talks",
        "summary_templates": [
            "The parties entered settlement discussions following negotiations over {topic}.",
            "Counsel for both sides reported renewed settlement talks tied to disputes over {topic}.",
        ],
        "relationships": [
            ("{plaintiff}", "entered_settlement_talks_with", "{defendant}"),
            ("Mediator", "facilitated", "{case_name}"),
        ],
        "entity_extras": ["Mediator", "Settlement Conference"],
    },
    "summary_judgment": {
        "title": "Summary Judgment Motion",
        "summary_templates": [
            "A summary judgment motion was filed seeking judgment on issues involving {topic}.",
            "{plaintiff} moved for summary judgment on a record centered on {topic}.",
        ],
        "relationships": [
            ("{plaintiff}", "moved_for_summary_judgment_in", "{case_name}"),
            ("{judge}", "considering", "Summary Judgment"),
        ],
        "entity_extras": ["Summary Judgment"],
    },
    "jury_verdict": {
        "title": "Jury Verdict",
        "summary_templates": [
            "A jury returned a verdict after reviewing evidence related to {topic}.",
            "The jury found on multiple claims after deliberation on allegations involving {topic}.",
        ],
        "relationships": [
            ("Jury", "returned_verdict_in", "{case_name}"),
            ("{judge}", "entered_verdict_in", "{case_name}"),
        ],
        "entity_extras": ["Jury", "Verdict Form"],
    },
    "sentencing": {
        "title": "Sentencing Hearing",
        "summary_templates": [
            "{judge} conducted sentencing proceedings following findings related to {topic}.",
            "The court imposed sentence after considering evidence tied to {topic}.",
        ],
        "relationships": [
            ("{judge}", "sentenced", "{defendant}"),
            ("{agency}", "prosecuted", "{case_name}"),
        ],
        "entity_extras": ["Sentencing Memorandum"],
    },
    "appeal_filed": {
        "title": "Appeal Filed",
        "summary_templates": [
            "A notice of appeal was filed challenging rulings concerning {topic}.",
            "{defendant} appealed the court's decision on issues related to {topic}.",
        ],
        "relationships": [
            ("{defendant}", "appealed", "{case_name}"),
            ("Court of Appeals", "reviewing", "{case_name}"),
        ],
        "entity_extras": ["Court of Appeals", "Notice of Appeal"],
    },
    "appellate_opinion": {
        "title": "Appellate Opinion",
        "summary_templates": [
            "An appellate opinion addressed legal questions arising from {topic}.",
            "The reviewing court issued an opinion clarifying standards related to {topic}.",
        ],
        "relationships": [
            ("Court of Appeals", "issued_opinion_in", "{case_name}"),
            ("{case_name}", "cites", "Precedent"),
        ],
        "entity_extras": ["Precedent", "Appellate Panel"],
    },
    "supreme_court_review": {
        "title": "High Court Review",
        "summary_templates": [
            "The high court agreed to review questions involving {topic}.",
            "Certiorari was granted on issues concerning {topic} in {case_name}.",
        ],
        "relationships": [
            ("Supreme Court", "reviewing", "{case_name}"),
            ("{case_name}", "raises_issue", "{topic}"),
        ],
        "entity_extras": ["Supreme Court", "Petition for Review"],
    },
    "regulatory_notice": {
        "title": "Regulatory Notice",
        "summary_templates": [
            "{agency} issued a notice connected to allegations involving {topic}.",
            "A regulatory notice was issued concerning reporting and disclosure obligations tied to {topic}.",
        ],
        "relationships": [
            ("{agency}", "issued_notice_involving", "{defendant}"),
            ("{agency}", "referenced", "{case_name}"),
        ],
        "entity_extras": ["Regulatory Notice"],
    },
    "witness_testimony": {
        "title": "Witness Testimony",
        "summary_templates": [
            "Witness testimony described internal events and decisions related to {topic}.",
            "A witness testified regarding communications, intent, and actions involving {topic}.",
        ],
        "relationships": [
            ("Witness", "testified_in", "{case_name}"),
            ("{judge}", "heard_testimony_in", "{case_name}"),
        ],
        "entity_extras": ["Witness", "Trial Testimony"],
    },
}

CLUSTER_LIBRARY: List[Dict] = [
    {
        "cluster_name": "Patent Litigation",
        "case_name_patterns": [
            "Apex Mobile v. Orion Devices",
            "Nimbus Systems v. Vertex Labs",
            "BluePeak Tech v. Helix Circuits",
            "Northstar Logic v. Quantum Handsets",
        ],
        "courts": [
            ("U.S. District Court for the Northern District of California", "district"),
            ("U.S. District Court for the District of Delaware", "district"),
        ],
        "parties_pool": [
            "Apex Mobile",
            "Orion Devices",
            "Nimbus Systems",
            "Vertex Labs",
            "BluePeak Tech",
            "Helix Circuits",
            "Northstar Logic",
            "Quantum Handsets",
        ],
        "judges": [
            "Judge Lucy Harrow",
            "Judge Elena Ward",
            "Judge Martin Cole",
        ],
        "agencies": [
            "U.S. Patent and Trademark Office",
        ],
        "topics": [
            "smartphone interface patents",
            "wireless chipset design",
            "user interface gestures",
            "battery optimization patents",
            "camera image processing patents",
        ],
        "event_flow": [
            "complaint_filed",
            "motion_to_dismiss",
            "discovery_order",
            "expert_report",
            "hearing",
            "summary_judgment",
            "jury_verdict",
            "appeal_filed",
            "appellate_opinion",
            "supreme_court_review",
        ],
        "bridge_entities": [
            "Supreme Court",
            "Court of Appeals",
            "Precedent",
        ],
    },
    {
        "cluster_name": "Corporate Fraud Trial",
        "case_name_patterns": [
            "United States v. Meridian BioLabs",
            "United States v. Pine Harbor Capital",
            "United States v. Sterling Diagnostics",
            "United States v. Rivergate Holdings",
        ],
        "courts": [
            ("U.S. District Court for the Northern District of California", "district"),
            ("U.S. District Court for the Southern District of New York", "district"),
        ],
        "parties_pool": [
            "United States",
            "Meridian BioLabs",
            "Pine Harbor Capital",
            "Sterling Diagnostics",
            "Rivergate Holdings",
            "Executive A",
            "Executive B",
            "Chief Financial Officer",
        ],
        "judges": [
            "Judge Edward Navarro",
            "Judge Tessa Monroe",
            "Judge Daniel Pike",
        ],
        "agencies": [
            "U.S. Department of Justice",
            "Securities and Exchange Commission",
            "Federal Trade Commission",
        ],
        "topics": [
            "investor disclosures",
            "financial reporting fraud",
            "clinical test misrepresentation",
            "internal controls failures",
            "wire fraud allegations",
        ],
        "event_flow": [
            "regulatory_notice",
            "complaint_filed",
            "witness_testimony",
            "discovery_order",
            "hearing",
            "expert_report",
            "jury_verdict",
            "sentencing",
            "appeal_filed",
            "appellate_opinion",
        ],
        "bridge_entities": [
            "Supreme Court",
            "Court of Appeals",
            "Investor Group",
        ],
    },
    {
        "cluster_name": "Copyright and Platform Dispute",
        "case_name_patterns": [
            "Atlas Software v. Nova Cloud",
            "CodeForge v. Open Runtime Labs",
            "Signal Grid v. Vector Stack",
            "SyntaxWorks v. Horizon Systems",
        ],
        "courts": [
            ("U.S. District Court for the Northern District of California", "district"),
            ("U.S. District Court for the Western District of Washington", "district"),
        ],
        "parties_pool": [
            "Atlas Software",
            "Nova Cloud",
            "CodeForge",
            "Open Runtime Labs",
            "Signal Grid",
            "Vector Stack",
            "SyntaxWorks",
            "Horizon Systems",
        ],
        "judges": [
            "Judge William Avery",
            "Judge Lena Brooks",
            "Judge Samuel Hart",
        ],
        "agencies": [
            "U.S. Copyright Office",
        ],
        "topics": [
            "API reuse",
            "software interoperability",
            "copyrightability of interfaces",
            "fair use in developer tools",
            "source code licensing",
        ],
        "event_flow": [
            "complaint_filed",
            "motion_to_dismiss",
            "hearing",
            "summary_judgment",
            "appeal_filed",
            "appellate_opinion",
            "supreme_court_review",
        ],
        "bridge_entities": [
            "Supreme Court",
            "Court of Appeals",
            "Developer Ecosystem",
        ],
    },
    {
        "cluster_name": "Antitrust and Market Conduct",
        "case_name_patterns": [
            "Federal Trade Commission v. Titan Search",
            "United States v. Metro Ad Exchange",
            "State Coalition v. Omnidata Retail",
            "Federal Trade Commission v. BrightCart Platform",
        ],
        "courts": [
            ("U.S. District Court for the District of Columbia", "district"),
            ("U.S. District Court for the Southern District of New York", "district"),
        ],
        "parties_pool": [
            "Federal Trade Commission",
            "United States",
            "Titan Search",
            "Metro Ad Exchange",
            "Omnidata Retail",
            "BrightCart Platform",
            "State Coalition",
        ],
        "judges": [
            "Judge Naomi Ellis",
            "Judge Robert Kline",
            "Judge Patrick Sloan",
        ],
        "agencies": [
            "Federal Trade Commission",
            "Department of Justice Antitrust Division",
        ],
        "topics": [
            "market foreclosure",
            "exclusive dealing",
            "self-preferencing",
            "ad exchange bidding practices",
            "platform monopoly allegations",
        ],
        "event_flow": [
            "complaint_filed",
            "regulatory_notice",
            "hearing",
            "discovery_order",
            "expert_report",
            "summary_judgment",
            "appeal_filed",
            "appellate_opinion",
        ],
        "bridge_entities": [
            "Court of Appeals",
            "Economic Expert",
            "Market Analysis",
        ],
    },
]


# ----------------------------
# Case Generation
# ----------------------------

def build_case_templates(case_count: int) -> List[CaseTemplate]:
    templates: List[CaseTemplate] = []
    years = [(2010, 2014), (2013, 2018), (2016, 2021), (2018, 2024)]

    for i in range(case_count):
        cluster = CLUSTER_LIBRARY[i % len(CLUSTER_LIBRARY)]
        case_name = random.choice(cluster["case_name_patterns"])
        court, court_type = random.choice(cluster["courts"])
        y1, y2 = random.choice(years)

        templates.append(
            CaseTemplate(
                cluster_name=cluster["cluster_name"],
                case_name=case_name,
                court=court,
                court_type=court_type,
                start_date=date(y1, 1, 1),
                end_date=date(y2, 12, 31),
                parties=pick_some(cluster["parties_pool"], min_n=2, max_n=4),
                judges=pick_some(cluster["judges"], min_n=1, max_n=2),
                agencies=pick_some(cluster["agencies"], min_n=1, max_n=min(2, len(cluster["agencies"]))),
                topics=pick_some(cluster["topics"], min_n=2, max_n=4),
                event_flow=cluster["event_flow"],
                bridge_entities=cluster["bridge_entities"],
            )
        )

    return templates


def normalize_parties(parties: List[str], cluster_name: str) -> Tuple[str, str]:
    unique = dedupe(parties)
    if cluster_name == "Corporate Fraud Trial":
        plaintiff = "United States"
        defendants = [p for p in unique if p != "United States"]
        defendant = random.choice(defendants) if defendants else "Executive A"
        return plaintiff, defendant

    plaintiff = unique[0] if unique else "Plaintiff"
    defendant = unique[1] if len(unique) > 1 else "Defendant"
    return plaintiff, defendant


def render_relationships(rel_specs: List[Tuple[str, str, str]], ctx: Dict[str, str]) -> List[Relationship]:
    rels: List[Relationship] = []
    for src, rel_type, tgt in rel_specs:
        rels.append(
            Relationship(
                source=src.format(**ctx),
                type=rel_type,
                target=tgt.format(**ctx),
            )
        )
    return rels


def create_document(
    doc_id: int,
    case: CaseTemplate,
    event_type: str,
) -> Dict:
    tpl = EVENT_TEMPLATES[event_type]
    plaintiff, defendant = normalize_parties(case.parties, case.cluster_name)
    judge = random.choice(case.judges)
    agency = random.choice(case.agencies)
    topic = random.choice(case.topics)
    dt = choose_date(case.start_date, case.end_date)

    ctx = {
        "plaintiff": plaintiff,
        "defendant": defendant,
        "judge": judge,
        "agency": agency,
        "topic": topic,
        "court": case.court,
        "case_name": case.case_name,
    }

    title = f"{case.case_name} — {tpl['title']}"
    summary = random.choice(tpl["summary_templates"]).format(**ctx)

    entities = [
        case.case_name,
        case.court,
        plaintiff,
        defendant,
        judge,
        agency,
        topic,
    ]
    entities.extend(case.bridge_entities)
    entities.extend(tpl["entity_extras"])

    if maybe(0.35):
        entities.extend(["Supreme Court", "Court of Appeals"])
    if maybe(0.25):
        entities.extend(["Investor Group", "Expert Witness", "Regulatory Notice"])

    relationships = render_relationships(tpl["relationships"], ctx)

    # Common graph-worthy edges
    common_relationships = [
        Relationship(source=case.case_name, type="filed_in", target=case.court),
        Relationship(source=case.case_name, type="belongs_to_cluster", target=case.cluster_name),
        Relationship(source=judge, type="assigned_to", target=case.case_name),
    ]
    relationships.extend(common_relationships)

    if maybe(0.45):
        relationships.append(Relationship(source=case.case_name, type="references_topic", target=topic))
    if "Supreme Court" in entities and maybe(0.20):
        relationships.append(Relationship(source="Supreme Court", type="reviewing", target=case.case_name))
    if "Court of Appeals" in entities and maybe(0.35):
        relationships.append(Relationship(source="Court of Appeals", type="reviewing", target=case.case_name))
    if agency and maybe(0.40):
        relationships.append(Relationship(source=agency, type="involved_in", target=case.case_name))

    source_type = random.choice([
        "Docket Entry Summary",
        "Court Filing Summary",
        "Opinion Summary",
        "Hearing Note",
        "Case Activity Update",
    ])

    sentiment = random.choice(["neutral", "procedural", "disputed", "adverse", "supportive"])
    importance = random.randint(1, 5)

    doc = {
        "id": f"doc_{doc_id:05d}",
        "title": title,
        "date": dt.isoformat(),
        "source": source_type,
        "cluster": case.cluster_name,
        "case_name": case.case_name,
        "court": case.court,
        "court_type": case.court_type,
        "event_type": event_type,
        "summary": summary,
        "entities": dedupe(sorted(entities)),
        "relationships": [r.as_dict() for r in relationships],
        "tags": dedupe([
            slugify(case.cluster_name),
            slugify(event_type),
            slugify(topic),
            "synthetic",
            "legal",
            "opensight-demo",
        ]),
        "analysis_hints": {
            "sentiment": sentiment,
            "importance": importance,
            "timeline_bucket": dt.year,
        },
    }
    return doc


# ----------------------------
# Dataset Assembly
# ----------------------------

def allocate_documents(total_docs: int, case_templates: List[CaseTemplate]) -> List[CaseTemplate]:
    """
    Spread documents across cases evenly, but allow some cases to get more docs.
    """
    result: List[CaseTemplate] = []
    while len(result) < total_docs:
        case = random.choice(case_templates)
        result.append(case)
    return result[:total_docs]


def event_for_case(case: CaseTemplate) -> str:
    return random.choice(case.event_flow)


def build_metadata(docs: List[Dict]) -> Dict:
    clusters: Dict[str, int] = {}
    event_types: Dict[str, int] = {}
    entities = set()
    cases = set()

    for doc in docs:
        clusters[doc["cluster"]] = clusters.get(doc["cluster"], 0) + 1
        event_types[doc["event_type"]] = event_types.get(doc["event_type"], 0) + 1
        cases.add(doc["case_name"])
        entities.update(doc.get("entities", []))

    dates = sorted(doc["date"] for doc in docs)

    return {
        "dataset_name": "OpenSight Synthetic Legal Dataset",
        "description": "Synthetic legal/court-style documents generated for graph, entity, and timeline testing in OpenSight.",
        "synthetic": True,
        "document_count": len(docs),
        "case_count": len(cases),
        "unique_entity_count": len(entities),
        "date_range": {
            "start": dates[0] if dates else None,
            "end": dates[-1] if dates else None,
        },
        "clusters": clusters,
        "event_types": event_types,
        "schema_version": "1.0",
        "required_fields": [
            "id",
            "title",
            "date",
            "source",
            "cluster",
            "case_name",
            "court",
            "event_type",
            "summary",
            "entities",
            "relationships",
        ],
    }


def generate_dataset(total_docs: int, case_count: int) -> List[Dict]:
    case_templates = build_case_templates(case_count=case_count)
    chosen_cases = allocate_documents(total_docs, case_templates)

    docs: List[Dict] = []
    for idx, case in enumerate(chosen_cases, start=1):
        doc = create_document(
            doc_id=idx,
            case=case,
            event_type=event_for_case(case),
        )
        docs.append(doc)

    docs.sort(key=lambda d: (d["date"], d["case_name"], d["id"]))
    return docs


# ----------------------------
# CLI
# ----------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic legal documents for OpenSight.")
    parser.add_argument(
        "--count",
        type=int,
        default=400,
        help="Number of documents to generate (default: 400).",
    )
    parser.add_argument(
        "--cases",
        type=int,
        default=24,
        help="Number of synthetic cases to distribute across (default: 24).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=1337,
        help="Random seed for reproducibility (default: 1337).",
    )
    parser.add_argument(
        "--out",
        type=str,
        default="examples/opensight_synthetic_legal_dataset",
        help="Output directory (default: examples/opensight_synthetic_legal_dataset).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    random.seed(args.seed)

    out_dir = Path(args.out)
    docs_dir = out_dir / "documents"
    docs_dir.mkdir(parents=True, exist_ok=True)

    docs = generate_dataset(total_docs=args.count, case_count=args.cases)
    metadata = build_metadata(docs)

    for doc in docs:
        json_write(docs_dir / f"{doc['id']}.json", doc)

    json_write(out_dir / "metadata.json", metadata)

    print(f"[+] Generated dataset at: {out_dir}")
    print(f"[+] Documents: {metadata['document_count']}")
    print(f"[+] Cases: {metadata['case_count']}")
    print(f"[+] Unique entities: {metadata['unique_entity_count']}")
    print(f"[+] Date range: {metadata['date_range']['start']} -> {metadata['date_range']['end']}")
    print("[+] Clusters:")
    for cluster, count in metadata["clusters"].items():
        print(f"    - {cluster}: {count}")

    print("\nSample ingest idea:")
    print(f"python scripts/data_ingest.py {docs_dir}")


if __name__ == "__main__":
    main()
