from dataclasses import dataclass
from typing import Literal, Optional

SearchStatus = Literal["queued", "running", "completed", "failed"]
Relevance = Literal["highly_relevant", "closely_related", "ignorable"]


@dataclass
class Search:
    search_id: str
    query: str
    status: SearchStatus
    accepted_at: str
    updated_at: str
    papers_scanned: int = 0
    relevant_found: int = 0
    completeness_estimate: float = 0.0
    error_message: Optional[str] = None


@dataclass
class SearchResult:
    search_id: str
    paper_id: str
    title: str
    score: float
    relevance: Relevance
    why: str


@dataclass
class Collection:
    collection_id: str
    name: str
    description: str
    created_at: str
