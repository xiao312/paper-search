from __future__ import annotations

import os
import urllib.parse
import urllib.request

from papersearch.ingest.http import get_json_with_retry


class SemanticScholarClient:
    BASE = "https://api.semanticscholar.org/graph/v1"

    def __init__(self, api_key: str | None = None, timeout: int = 15):
        self.api_key = api_key or os.getenv("SEMANTIC_SCHOLAR_API_KEY")
        self.timeout = timeout

    def search(self, query: str, limit: int = 10, use_mock: bool = False) -> list[dict]:
        if use_mock:
            return self._mock(query, limit)

        fields = "paperId,title,abstract,year,venue,doi,url,authors"
        qs = urllib.parse.urlencode({"query": query, "limit": max(1, min(limit, 100)), "fields": fields})
        url = f"{self.BASE}/paper/search?{qs}"

        headers = {"Accept": "application/json"}
        if self.api_key:
            headers["x-api-key"] = self.api_key

        req = urllib.request.Request(url, headers=headers, method="GET")
        payload = get_json_with_retry(req, timeout=self.timeout, retries=3)

        out = []
        for item in payload.get("data", []):
            doi = item.get("doi")
            out.append(
                {
                    "paper_id": item.get("paperId"),
                    "doi": doi,
                    "title": item.get("title") or "",
                    "abstract": item.get("abstract") or "",
                    "year": item.get("year"),
                    "venue": item.get("venue"),
                    "url": item.get("url"),
                    "authors": [a.get("name") for a in item.get("authors", []) if a.get("name")],
                    "source": "semanticscholar",
                }
            )
        return out

    @staticmethod
    def _mock(query: str, limit: int) -> list[dict]:
        out = []
        for i in range(max(1, limit)):
            doi = f"10.1016/j.mock.{2020 + i % 5}.{100000 + i}" if i % 2 == 0 else f"10.48550/arXiv.{2401 + i}.{1000 + i}"
            out.append(
                {
                    "paper_id": f"S2_MOCK_{i}",
                    "doi": doi,
                    "title": f"{query} mock candidate {i + 1}",
                    "abstract": f"Mock abstract for {query}, candidate {i + 1}.",
                    "year": 2020 + i % 5,
                    "venue": "Mock Venue",
                    "url": "https://example.org/mock",
                    "authors": ["Alice", "Bob"],
                    "source": "semanticscholar-mock",
                }
            )
        return out
