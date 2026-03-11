from __future__ import annotations

import urllib.parse
import urllib.request

from papersearch.ingest.errors import ProviderError
from papersearch.ingest.http import get_json_with_retry


class CrossrefClient:
    BASE = "https://api.crossref.org/works"

    def __init__(self, timeout: int = 20):
        self.timeout = timeout

    @staticmethod
    def _norm_doi(value: str | None) -> str | None:
        if not value:
            return None
        v = value.strip().lower()
        if v.startswith("https://doi.org/"):
            v = v[len("https://doi.org/") :]
        if v.startswith("http://doi.org/"):
            v = v[len("http://doi.org/") :]
        if v.startswith("doi:"):
            v = v[4:]
        return v or None

    def search_works(self, query: str, rows: int = 30) -> dict:
        q = (query or "").strip()
        if len(q) < 3:
            return {"source": "crossref", "query": q, "items": [], "error": "query_too_short"}

        params = urllib.parse.urlencode({"query": q, "rows": max(1, min(int(rows), 100))})
        url = f"{self.BASE}?{params}"
        req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "curl/8.5.0"}, method="GET")
        try:
            payload = get_json_with_retry(req, timeout=self.timeout, retries=2)
        except ProviderError as e:
            return {"source": "crossref", "query": q, "items": [], "error": str(e)}

        msg = payload.get("message", {}) or {}
        out = []
        for it in msg.get("items", []) or []:
            title = (it.get("title") or [""])
            container = (it.get("container-title") or [""])
            date_parts = ((it.get("issued") or {}).get("date-parts") or [])
            published = None
            if date_parts and date_parts[0]:
                published = "-".join(str(x) for x in date_parts[0])
            out.append(
                {
                    "doi": self._norm_doi(it.get("DOI")),
                    "title": title[0] if title else "",
                    "journal": container[0] if container else "",
                    "published": published,
                    "score": it.get("score"),
                    "type": it.get("type"),
                    "url": it.get("URL"),
                    "is_referenced_by_count": it.get("is-referenced-by-count"),
                }
            )

        return {
            "source": "crossref",
            "query": q,
            "items": out,
            "total_results": msg.get("total-results"),
            "error": None,
        }

    def references_by_doi(self, doi: str) -> dict:
        doi = (doi or "").strip().lower()
        if not doi:
            return {"source": "crossref", "references": [], "reference_count": 0, "citation_count": 0, "error": "missing_doi"}

        url = f"{self.BASE}/{urllib.parse.quote(doi, safe='')}"
        req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "curl/8.5.0"}, method="GET")
        try:
            payload = get_json_with_retry(req, timeout=self.timeout, retries=2)
        except ProviderError as e:
            return {"source": "crossref", "references": [], "reference_count": 0, "citation_count": 0, "error": str(e)}

        msg = payload.get("message", {})
        out_refs = []
        for r in msg.get("reference", []) or []:
            doi_ref = (r.get("DOI") or "").strip().lower() or None
            raw = (r.get("unstructured") or "").strip()
            if not raw:
                parts = [r.get("author"), r.get("article-title"), str(r.get("year") or "").strip()]
                raw = " ".join(x for x in parts if x).strip()
            out_refs.append({"doi": doi_ref, "raw_text": raw})

        title = ((msg.get("title") or [""])[0] or "").strip()
        venue = ((msg.get("container-title") or [""])[0] or "").strip()
        year = None
        date_parts = ((msg.get("issued") or {}).get("date-parts") or [])
        if date_parts and date_parts[0]:
            try:
                year = int(date_parts[0][0])
            except Exception:
                year = None

        return {
            "source": "crossref",
            "references": out_refs,
            "reference_count": int(msg.get("reference-count") or len(out_refs)),
            "citation_count": int(msg.get("is-referenced-by-count") or 0),
            "title": title,
            "year": year,
            "venue": venue,
            "error": None,
        }
