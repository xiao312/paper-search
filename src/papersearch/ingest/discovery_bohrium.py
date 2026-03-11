from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request

from papersearch.ingest.errors import ProviderError
from papersearch.ingest.http import get_json_with_retry


class BohriumSigmaSearchClient:
    BASE = "https://open.bohrium.com/openapi/v1/sigma-search/api/v1/ai_search"

    def __init__(self, access_key: str | None = None, timeout: int = 20):
        self.access_key = access_key or os.getenv("BOHRIUM_ACCESS_KEY") or os.getenv("ACCESS_KEY")
        self.timeout = timeout

    def create_session(
        self,
        query: str,
        model: str = "auto",
        discipline: str = "All",
        resource_id_list: list[str] | None = None,
        access_key: str | None = None,
    ) -> dict:
        q = (query or "").strip()
        if len(q) < 3:
            raise ValueError("query must be at least 3 chars")

        key = access_key or self.access_key
        params: dict[str, str] = {}
        if key:
            params["accessKey"] = key
        qs = urllib.parse.urlencode(params)

        url = f"{self.BASE}/sessions"
        if qs:
            url = f"{url}?{qs}"

        body = {
            "query": q,
            "model": model,
            "discipline": discipline,
            "resource_id_list": resource_id_list or [],
        }

        req = urllib.request.Request(
            url,
            data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
            headers={"Accept": "application/json", "Content-Type": "application/json", "User-Agent": "curl/8.5.0"},
            method="POST",
        )

        try:
            payload = get_json_with_retry(req, timeout=self.timeout, retries=2)
        except ProviderError as e:
            return {"query": q, "uuid": None, "title": None, "share": None, "code": None, "error": str(e)}

        data = payload.get("data", {}) or {}
        return {
            "query": q,
            "uuid": data.get("uuid"),
            "title": data.get("title"),
            "share": data.get("share"),
            "code": payload.get("code"),
            "error": None,
        }

    def get_session_detail(self, uuid: str, access_key: str | None = None) -> dict:
        uid = (uuid or "").strip()
        if not uid:
            raise ValueError("uuid is required")

        key = access_key or self.access_key
        params: dict[str, str] = {}
        if key:
            params["accessKey"] = key
        qs = urllib.parse.urlencode(params)

        path_uid = urllib.parse.quote(uid, safe="")
        url = f"{self.BASE}/sessions/{path_uid}"
        if qs:
            url = f"{url}?{qs}"

        req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "curl/8.5.0"}, method="GET")

        try:
            payload = get_json_with_retry(req, timeout=self.timeout, retries=2)
        except ProviderError as e:
            return {"uuid": uid, "code": None, "questions": [], "query_id": None, "error": str(e)}

        data = payload.get("data", {}) or {}
        questions = list(data.get("questions") or [])
        query_id = None
        if questions:
            query_id = questions[0].get("id")

        return {
            "uuid": data.get("uuid") or uid,
            "title": data.get("title"),
            "status": data.get("status"),
            "model": data.get("model"),
            "discipline": data.get("discipline"),
            "questions": questions,
            "query_id": str(query_id) if query_id is not None else None,
            "code": payload.get("code"),
            "error": None,
        }

    def question_papers(self, query_id: str, sort: str | None = None, access_key: str | None = None) -> dict:
        qid = (query_id or "").strip()
        if not qid:
            raise ValueError("query_id is required")

        key = access_key or self.access_key
        params: dict[str, str] = {}
        if sort:
            params["sort"] = sort
        if key:
            params["accessKey"] = key

        path_qid = urllib.parse.quote(qid, safe="")
        qs = urllib.parse.urlencode(params)
        url = f"{self.BASE}/questions/{path_qid}/papers"
        if qs:
            url = f"{url}?{qs}"

        req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "curl/8.5.0"}, method="GET")
        try:
            payload = get_json_with_retry(req, timeout=self.timeout, retries=2)
        except ProviderError as e:
            return {
                "query_id": qid,
                "source": "bohrium-sigma-search",
                "sort": sort,
                "items": [],
                "source_list": [],
                "log_id": None,
                "error": str(e),
            }

        data = payload.get("data", {}) or {}
        out_items = []
        for it in data.get("list", []) or []:
            out_items.append(
                {
                    "sequence_id": it.get("sequenceId"),
                    "title": it.get("title") or "",
                    "title_zh": it.get("titleZh") or "",
                    "abstract": it.get("abstract") or "",
                    "abstract_zh": it.get("abstractZh") or "",
                    "doi": (it.get("doi") or "").strip().lower() or None,
                    "arxiv": it.get("arxiv") or None,
                    "journal": it.get("journal") or "",
                    "publication_date": it.get("publicationDate"),
                    "authors": list(it.get("author") or []),
                    "relevance_score": it.get("relevanceScore"),
                    "sort_score": it.get("sortScore"),
                    "citation_nums": it.get("citationNums"),
                    "impact_factor": it.get("impactFactor"),
                    "source": it.get("source") or "",
                    "link": it.get("link") or "",
                    "open_access": it.get("openAccess"),
                    "pdf_flag": it.get("pdfFlag"),
                    "bohrium_id": it.get("bohriumId"),
                    "ai_summarize": it.get("aiSummarize") or "",
                }
            )

        return {
            "query_id": qid,
            "source": "bohrium-sigma-search",
            "sort": sort,
            "items": out_items,
            "source_list": list(data.get("sourceList") or []),
            "log_id": data.get("logId"),
            "code": payload.get("code"),
            "error": None,
        }
