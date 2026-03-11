from __future__ import annotations

import urllib.parse
import urllib.request

from papersearch.ingest.errors import ProviderError
from papersearch.ingest.http import get_json_with_retry


class OpenAlexClient:
    BASE = "https://api.openalex.org"

    def __init__(self, timeout: int = 20):
        self.timeout = timeout

    @staticmethod
    def _abstract_from_inverted_index(inv: dict | None) -> str:
        if not isinstance(inv, dict) or not inv:
            return ""
        pos_to_token: dict[int, str] = {}
        for token, pos_list in inv.items():
            for p in pos_list or []:
                try:
                    pos_to_token[int(p)] = str(token)
                except Exception:
                    pass
        if not pos_to_token:
            return ""
        return " ".join(pos_to_token[i] for i in sorted(pos_to_token.keys())).strip()

    @staticmethod
    def _norm_doi(value: str | None) -> str | None:
        if not value:
            return None
        v = value.strip().lower()
        if v.startswith("https://doi.org/"):
            v = v[len("https://doi.org/") :]
        if v.startswith("doi:"):
            v = v[4:]
        return v or None

    @staticmethod
    def _openalex_id(value: str | None) -> str | None:
        if not value:
            return None
        s = value.strip()
        if not s:
            return None
        if s.startswith("https://openalex.org/"):
            return s.split("/")[-1]
        return s

    def search_sources(self, name: str, limit: int = 5) -> list[dict]:
        q = urllib.parse.urlencode({"search": name, "per-page": max(1, min(limit, 25))})
        url = f"{self.BASE}/sources?{q}"
        req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "curl/8.5.0"}, method="GET")
        try:
            payload = get_json_with_retry(req, timeout=self.timeout, retries=2)
        except ProviderError:
            return []
        return payload.get("results", []) or []

    def resolve_source_id(self, name: str) -> str | None:
        name_norm = (name or "").strip().lower()
        if not name_norm:
            return None
        cands = self.search_sources(name, limit=10)
        if not cands:
            return None

        exact = [c for c in cands if (c.get("display_name") or "").strip().lower() == name_norm]
        chosen = exact[0] if exact else cands[0]
        sid = chosen.get("id") or ""
        return sid.split("/")[-1] if sid else None

    def iter_works_by_source(self, source_id: str, per_page: int = 200):
        cursor = "*"
        per_page = max(1, min(int(per_page), 200))
        while True:
            q = urllib.parse.urlencode(
                {
                    "filter": f"primary_location.source.id:https://openalex.org/{source_id}",
                    "per-page": per_page,
                    "cursor": cursor,
                }
            )
            url = f"{self.BASE}/works?{q}"
            req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "curl/8.5.0"}, method="GET")
            payload = get_json_with_retry(req, timeout=self.timeout, retries=2)
            results = payload.get("results", []) or []
            if not results:
                break
            yield results, payload.get("meta", {}) or {}
            cursor = (payload.get("meta", {}) or {}).get("next_cursor")
            if not cursor:
                break

    def works_by_source(self, source_id: str, max_results: int = 25) -> list[dict]:
        out = []
        remaining = max(1, max_results)
        try:
            for results, _meta in self.iter_works_by_source(source_id=source_id, per_page=min(remaining, 200)):
                batch = results[:remaining]
                out.extend(batch)
                remaining -= len(batch)
                if remaining <= 0:
                    break
        except ProviderError:
            return out
        return out[:max_results]

    def work_by_doi(self, doi: str) -> dict:
        doi = (doi or "").strip().lower()
        if not doi:
            return {"source": "openalex", "doi": None, "title": "", "abstract": "", "year": None, "venue": "", "error": "missing_doi"}

        work_url = f"{self.BASE}/works/https://doi.org/{urllib.parse.quote(doi, safe='')}"
        req = urllib.request.Request(work_url, headers={"Accept": "application/json", "User-Agent": "curl/8.5.0"}, method="GET")
        try:
            work = get_json_with_retry(req, timeout=self.timeout, retries=2)
        except ProviderError as e:
            return {"source": "openalex", "doi": doi, "title": "", "abstract": "", "year": None, "venue": "", "error": str(e)}

        return {
            "source": "openalex",
            "doi": self._norm_doi(work.get("doi")) or doi,
            "title": (work.get("display_name") or "").strip(),
            "abstract": self._abstract_from_inverted_index(work.get("abstract_inverted_index")),
            "year": work.get("publication_year"),
            "venue": (((work.get("primary_location") or {}).get("source") or {}).get("display_name") or "").strip(),
            "citation_count": int(work.get("cited_by_count") or 0),
            "reference_count": int(work.get("referenced_works_count") or 0),
            "error": None,
        }

    def references_by_doi(self, doi: str) -> dict:
        doi = (doi or "").strip().lower()
        if not doi:
            return {"source": "openalex", "references": [], "reference_count": 0, "citation_count": 0, "error": "missing_doi"}

        work_url = f"{self.BASE}/works/https://doi.org/{urllib.parse.quote(doi, safe='')}"
        req = urllib.request.Request(work_url, headers={"Accept": "application/json", "User-Agent": "curl/8.5.0"}, method="GET")
        try:
            work = get_json_with_retry(req, timeout=self.timeout, retries=2)
        except ProviderError as e:
            return {"source": "openalex", "references": [], "reference_count": 0, "citation_count": 0, "error": str(e)}

        ref_ids = [x.split("/")[-1] for x in (work.get("referenced_works") or []) if x]
        citation_count = int(work.get("cited_by_count") or 0)

        refs = []
        for i in range(0, len(ref_ids), 25):
            batch = ref_ids[i : i + 25]
            if not batch:
                continue
            q = "|".join(batch)
            u = f"{self.BASE}/works?filter=openalex_id:{urllib.parse.quote(q, safe='|:')}&per-page={len(batch)}"
            r = urllib.request.Request(u, headers={"Accept": "application/json", "User-Agent": "curl/8.5.0"}, method="GET")
            try:
                payload = get_json_with_retry(r, timeout=self.timeout, retries=2)
            except ProviderError:
                continue
            for it in payload.get("results", []) or []:
                doi_ref = self._norm_doi(it.get("doi"))
                refs.append({"doi": doi_ref, "raw_text": (it.get("display_name") or "").strip()})

        return {
            "source": "openalex",
            "references": refs,
            "reference_count": len(ref_ids),
            "citation_count": citation_count,
            "error": None,
        }
