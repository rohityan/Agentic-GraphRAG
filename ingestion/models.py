"""Centralized Vertex AI Structured Output Schemas."""
from typing import Literal, Dict, List
from pydantic import BaseModel, Field

class IssueSummaryResponse(BaseModel):
    issue_type: Literal["Bug", "Feature", "Documentation", "Other", "Unknown"]
    environment_versions: Dict[str, str] = Field(default_factory=dict)
    issue_summary: str
    triage_category: str
    has_maintainer_resolution: bool
    resolution_snippet: str

class PRSummaryResponse(BaseModel):
    pr_type: Literal["Bug", "Feature", "Documentation", "Other", "Unknown"]
    pr_summary: str
    modified_symbols: List[str] = Field(default_factory=list)
    triage_category: str

class FeatureSummary(BaseModel):
    summary: str