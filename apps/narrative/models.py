from __future__ import annotations
from typing import List, Optional
from pydantic import BaseModel, Field

class ClaimExtraction(BaseModel):
    subject: str = ""
    predicate: str = ""
    object: str = ""
    speaker: str = ""
    target: str = ""
    claim_type: str = "statement"
    polarity: str = "affirmed"
    certainty: str = "medium"
    event_time_text: str = ""
    location_text: str = ""
    summary: str = ""
    quote: str = ""
    confidence: float = 0.5

class ClaimExtractionBatch(BaseModel):
    claims: List[ClaimExtraction] = Field(default_factory=list)

class NarrativeRebuildRequest(BaseModel):
    source_tag: Optional[str] = None
    max_units: Optional[int] = None
    rebuild_edges: bool = True
