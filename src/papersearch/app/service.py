from __future__ import annotations

import hashlib
import json
import math
import random
import re
import time
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from difflib import SequenceMatcher
from typing import Any, Optional
from uuid import uuid4

from papersearch.app.repository import Repo
from papersearch.app.run_manager import RunManager
from papersearch.adapters.feishu.notifier import FeishuNotifier
from papersearch.ingest.discovery_bohrium import BohriumSigmaSearchClient
from papersearch.ingest.discovery_crossref import CrossrefClient
from papersearch.ingest.discovery_openalex import OpenAlexClient
from papersearch.ingest.errors import ProviderError
from papersearch.ingest.fetch_elsevier_xml import ElsevierFullTextClient
from papersearch.ingest.pipeline import ingest_doi
from papersearch.ingest.xml_extractors import extract_abstract
from papersearch.integrations.pi_mono_client import PiMonoClient


DEFAULT_LLM_PROVIDER = "zai"
DEFAULT_LLM_MODEL = "glm-4.5-flash"
DEFAULT_LLM_THINKING = "off"


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _year_from_metadata(normalized: dict) -> int | None:
    md = normalized.get("metadata") or {}
    cover = (md.get("cover_date") or "").strip()
    if len(cover) >= 4 and cover[:4].isdigit():
        return int(cover[:4])
    return None


def _normalize_doi(value: str | None) -> str | None:
    if not value:
        return None
    v = value.strip().lower()
    if v.startswith("https://doi.org/"):
        v = v[len("https://doi.org/") :]
    if v.startswith("http://doi.org/"):
        v = v[len("http://doi.org/") :]
    v = v.rstrip(" .,;)")
    if v.startswith("doi:"):
        v = v[4:].strip()
    return v or None


def _extract_doi(text: str | None) -> str | None:
    if not text:
        return None
    m = re.search(r"10\.\d{4,9}/[-._;()/:a-z0-9]+", text, flags=re.I)
    return _normalize_doi(m.group(0)) if m else None


def _is_likely_doi(value: str | None) -> bool:
    v = _normalize_doi(value)
    if not v:
        return False
    return bool(re.match(r"^10\.\d{4,9}/\S+$", v))


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _year_from_any(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value if 1800 <= value <= 2200 else None
    s = str(value).strip()
    if len(s) >= 4 and s[:4].isdigit():
        y = int(s[:4])
        if 1800 <= y <= 2200:
            return y
    return None


def _ms_since(t0: float) -> int:
    return int((time.perf_counter() - t0) * 1000)


def _parse_json_object(text: str | None) -> dict | None:
    if not text:
        return None
    s = (text or "").strip()
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:
        pass
    m = re.search(r"\{[\s\S]*\}", s)
    if not m:
        return None
    try:
        obj = json.loads(m.group(0))
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _parse_json_array(text: str | None) -> list | None:
    if not text:
        return None
    s = (text or "").strip()
    try:
        arr = json.loads(s)
        return arr if isinstance(arr, list) else None
    except Exception:
        pass
    m = re.search(r"\[[\s\S]*\]", s)
    if not m:
        return None
    try:
        arr = json.loads(m.group(0))
        return arr if isinstance(arr, list) else None
    except Exception:
        return None


def _pick_count_primary_fallback(primary: Any, fallback: Any) -> int:
    p = int(_safe_float(primary, 0.0))
    if p > 0:
        return p
    f = int(_safe_float(fallback, 0.0))
    return max(f, 0)


def _normalize_text(s: str | None) -> str:
    x = (s or "").lower()
    x = re.sub(r"[^a-z0-9]+", " ", x)
    return " ".join(x.split())


def _title_similarity(a: str | None, b: str | None) -> float:
    aa = _normalize_text(a)
    bb = _normalize_text(b)
    if not aa or not bb:
        return 0.0
    if aa == bb:
        return 1.0
    return float(SequenceMatcher(None, aa, bb).ratio())


def _extract_quoted_titles(query: str) -> list[str]:
    out = []
    for m in re.findall(r"[\"']([^\"']{20,300})[\"']", query or ""):
        t = (m or "").strip()
        if t:
            out.append(t)
    # de-dup preserve order
    seen = set()
    uniq = []
    for t in out:
        k = _normalize_text(t)
        if k in seen:
            continue
        seen.add(k)
        uniq.append(t)
    return uniq


def _merge_reference_rows(base_refs: list[dict], extra_refs: list[dict]) -> list[dict]:
    merged = []
    seen_doi = set()
    seen_text = set()

    for r in base_refs + extra_refs:
        doi = _normalize_doi(r.get("doi"))
        raw = (r.get("raw_text") or "").strip()
        if doi and doi in seen_doi:
            continue
        if not doi and raw and raw.lower() in seen_text:
            continue

        merged.append({"doi": doi, "raw_text": raw})
        if doi:
            seen_doi.add(doi)
        if raw:
            seen_text.add(raw.lower())

    return merged


class AppService:
    def __init__(self, repo: Optional[Repo] = None, notifier: Optional[FeishuNotifier] = None, run_manager: Optional[RunManager] = None):
        self.repo = repo or Repo()
        self.notifier = notifier
        self.run_manager = run_manager or RunManager()

    def start_search(self, query: str, limit: int = 20) -> dict:
        if not query or len(query.strip()) < 3:
            raise ValueError("query must be at least 3 chars")

        search_id = f"srch_{uuid4().hex[:12]}"
        now = _now_iso()
        self.repo.insert_search(
            {
                "search_id": search_id,
                "query": query,
                "status": "queued",
                "accepted_at": now,
                "updated_at": now,
                "papers_scanned": 0,
                "relevant_found": 0,
                "completeness_estimate": 0.0,
                "error_message": None,
            }
        )

        self.repo.update_search(search_id, status="running", updated_at=_now_iso())
        results = self._generate_results(search_id, query, n=max(10, min(limit, 50)))
        relevant = sum(1 for r in results if r["relevance"] in ("highly_relevant", "closely_related"))
        self.repo.insert_results(results)
        self.repo.update_search(
            search_id,
            status="completed",
            updated_at=_now_iso(),
            papers_scanned=len(results),
            relevant_found=relevant,
            completeness_estimate=0.92,
        )

        if self.notifier:
            self.notifier.notify_search_completed(search_id=search_id, query=query, relevant_found=relevant, completeness=0.92)

        return {
            "search_id": search_id,
            "status": "completed",
            "accepted_at": now,
            "updated_at": _now_iso(),
            "papers_scanned": len(results),
            "relevant_found": relevant,
            "completeness_estimate": 0.92,
        }

    def get_search_status(self, search_id: str) -> dict:
        row = self.repo.get_search(search_id)
        if not row:
            raise KeyError("search not found")
        return {
            "search_id": row["search_id"],
            "status": row["status"],
            "progress": {
                "papers_scanned": row["papers_scanned"],
                "relevant_found": row["relevant_found"],
            },
            "completeness": {
                "estimate": row["completeness_estimate"],
                "method": "discovery_curve_v1",
            },
            "error": row["error_message"],
        }

    def get_search_results(self, search_id: str, limit: int = 20, cursor: Optional[str] = None) -> dict:
        if not self.repo.get_search(search_id):
            raise KeyError("search not found")
        int_cursor = int(cursor) if cursor else 0
        rows, next_cursor = self.repo.list_results(search_id, limit=limit, cursor=int_cursor)
        items = [
            {
                "paper_id": r["paper_id"],
                "title": r["title"],
                "score": round(r["score"], 4),
                "relevance": r["relevance"],
                "why": r["why"],
            }
            for r in rows
        ]
        return {"search_id": search_id, "items": items, "next_cursor": str(next_cursor) if next_cursor else None}

    def create_collection(self, name: str, description: str = "") -> dict:
        if not name.strip():
            raise ValueError("name is required")
        collection_id = f"col_{uuid4().hex[:12]}"
        created_at = _now_iso()
        self.repo.create_collection(
            {
                "collection_id": collection_id,
                "name": name.strip(),
                "description": description,
                "created_at": created_at,
            }
        )
        return {"collection_id": collection_id, "name": name.strip(), "created_at": created_at}

    def add_paper_to_collection(self, collection_id: str, paper_id: str, note: str = "") -> dict:
        if not self.repo.get_collection(collection_id):
            raise KeyError("collection not found")
        self.repo.add_paper_to_collection(collection_id, paper_id, note)
        return {"collection_id": collection_id, "paper_id": paper_id, "added": True}

    def save_paper(self, paper_id: str, collection_id: Optional[str] = None) -> dict:
        if collection_id:
            return self.add_paper_to_collection(collection_id, paper_id)
        return {"paper_id": paper_id, "saved": True, "collection_id": None}

    def bohrium_create_session(
        self,
        query: str,
        model: str = "auto",
        discipline: str = "All",
        resource_id_list: list[str] | None = None,
        access_key: str | None = None,
    ) -> dict:
        client = BohriumSigmaSearchClient(access_key=access_key)
        return client.create_session(
            query=query,
            model=model,
            discipline=discipline,
            resource_id_list=resource_id_list or [],
            access_key=access_key,
        )

    def bohrium_session_detail(self, uuid: str, access_key: str | None = None) -> dict:
        client = BohriumSigmaSearchClient(access_key=access_key)
        return client.get_session_detail(uuid=uuid, access_key=access_key)

    def bohrium_question_papers(self, query_id: str, sort: str = "RelevanceScore", access_key: str | None = None) -> dict:
        client = BohriumSigmaSearchClient(access_key=access_key)
        return client.question_papers(query_id=query_id, sort=sort, access_key=access_key)

    def _resolve_llm_config(
        self,
        provider: str | None = None,
        model: str | None = None,
        thinking: str | None = None,
    ) -> tuple[str, str, str]:
        p = (provider or "").strip().lower() or DEFAULT_LLM_PROVIDER
        m = (model or "").strip() or DEFAULT_LLM_MODEL
        t = (thinking or "").strip().lower() or DEFAULT_LLM_THINKING

        # Hard policy: never use OpenAI provider/models in this project runtime.
        if p.startswith("openai") or m.lower().startswith("gpt-"):
            p = DEFAULT_LLM_PROVIDER
            m = DEFAULT_LLM_MODEL

        if t == "none":
            t = "off"
        if t not in ("off", "minimal", "low", "medium", "high", "xhigh"):
            t = DEFAULT_LLM_THINKING

        return p, m, t

    def llm_list_models(self, provider: str | None = None, search: str | None = None) -> dict:
        provider_resolved, _, _ = self._resolve_llm_config(provider=provider, model=None, thinking=None)
        client = PiMonoClient()
        out = client.list_models(provider=provider_resolved, search=search)
        lines = [x for x in out.stdout.splitlines() if x.strip()]
        return {
            "ok": out.ok,
            "provider": provider_resolved,
            "search": search,
            "model_lines": lines,
            "stdout": out.stdout,
            "stderr": out.stderr,
            "returncode": out.returncode,
            "command": out.command,
        }

    def llm_prompt(
        self,
        prompt: str,
        provider: str | None = None,
        model: str | None = None,
        thinking: str | None = None,
    ) -> dict:
        provider_resolved, model_resolved, thinking_resolved = self._resolve_llm_config(provider=provider, model=model, thinking=thinking)
        client = PiMonoClient()
        out = client.prompt(prompt=prompt, provider=provider_resolved, model=model_resolved, thinking=thinking_resolved)
        return {
            "ok": out.ok,
            "provider": provider_resolved,
            "model": model_resolved,
            "thinking": thinking_resolved,
            "response": out.stdout,
            "stderr": out.stderr,
            "returncode": out.returncode,
            "command": out.command,
        }

    def _paper_item_to_seed(self, it: dict, source_override: str | None = None) -> dict | None:
        doi = _normalize_doi(it.get("doi"))
        arxiv = (it.get("arxiv") or "").strip() or None
        if not doi and not arxiv:
            return None
        return {
            "paper_id": (f"doi:{doi}" if doi else f"arxiv:{arxiv}"),
            "doi": doi,
            "arxiv": arxiv,
            "title": it.get("title") or "",
            "journal": it.get("journal") or "",
            "publication_date": it.get("publication_date"),
            "relevance_score": it.get("relevance_score"),
            "sort_score": it.get("sort_score"),
            "source": source_override or it.get("source"),
            "link": it.get("link"),
        }

    def _merge_unique_seeds(self, base: list[dict], add: list[dict], limit: int) -> list[dict]:
        out = []
        seen = set()
        for s in (base or []) + (add or []):
            pid = (s.get("paper_id") or "").strip().lower()
            if not pid or pid in seen:
                continue
            seen.add(pid)
            out.append(s)
            if len(out) >= limit:
                break
        return out

    def _mentioned_seed_candidates(self, query: str, items: list[dict]) -> list[dict]:
        q = query or ""
        out: list[dict] = []

        # DOI mention has highest priority
        doi = _extract_doi(q)
        if doi:
            for it in items:
                if _normalize_doi(it.get("doi")) == doi:
                    s = self._paper_item_to_seed(it)
                    if s:
                        out.append(s)
                        return out
            row = self.repo.get_api_paper_by_doi(doi)
            if row:
                out.append(
                    {
                        "paper_id": str(row["paper_id"]),
                        "doi": str(row["doi"]).lower(),
                        "arxiv": None,
                        "title": str(row["title"]),
                        "journal": str(row["venue"] or ""),
                        "publication_date": None,
                        "relevance_score": None,
                        "sort_score": None,
                        "source": "local_db_match",
                        "link": f"https://doi.org/{str(row['doi']).lower()}",
                    }
                )
                return out

        for title in _extract_quoted_titles(q):
            # exact/fuzzy match from current Bohrium items first
            best = None
            best_sim = 0.0
            for it in items:
                sim = _title_similarity(title, it.get("title") or "")
                if sim > best_sim:
                    best_sim = sim
                    best = it
            if best is not None and best_sim >= 0.86:
                s = self._paper_item_to_seed(best)
                if s:
                    out.append(s)
                    continue

            # fallback to local database fuzzy/precise lookup
            rows = self.repo.find_api_papers_by_title_like(title, limit=10)
            db_best = None
            db_best_sim = 0.0
            for r in rows:
                sim = _title_similarity(title, r["title"])
                if sim > db_best_sim:
                    db_best_sim = sim
                    db_best = r
            if db_best is not None and db_best_sim >= 0.82:
                doi2 = _normalize_doi(db_best["doi"])
                out.append(
                    {
                        "paper_id": f"doi:{doi2}",
                        "doi": doi2,
                        "arxiv": None,
                        "title": str(db_best["title"]),
                        "journal": str(db_best["venue"] or ""),
                        "publication_date": None,
                        "relevance_score": None,
                        "sort_score": None,
                        "source": "local_db_match",
                        "link": f"https://doi.org/{doi2}" if doi2 else None,
                    }
                )

        return out

    def _fallback_query_variants(self, query: str, expanded_query: str) -> list[str]:
        base = []
        if expanded_query and expanded_query.strip() and expanded_query.strip() != query.strip():
            base.append(expanded_query.strip())

        titles = _extract_quoted_titles(query)
        if titles:
            base.append(titles[0])

        key_query = "ammonia natural gas combustion kinetics deep learning surrogate stiff ODE CFD"
        if "ammonia" in (query or "").lower():
            base.append(key_query)

        # soft-compressed version of long prompt
        compact = re.sub(r"\s+", " ", (query or "").strip())
        if len(compact) > 280:
            compact = compact[:280].rsplit(" ", 1)[0]
            if compact:
                base.append(compact)

        # de-dup preserve order
        out = []
        seen = set()
        for q in base:
            k = _normalize_text(q)
            if not k or k in seen:
                continue
            seen.add(k)
            out.append(q)
        return out

    def _topup_seeds_from_crossref(self, query: str, expanded_query: str, rows: int, top_k: int) -> tuple[list[dict], dict]:
        client = CrossrefClient()

        # user query first: empirically better for long contextual prompts
        attempts = []
        merged_items = []
        seen = set()

        for q in [query, expanded_query]:
            qq = (q or "").strip()
            if len(qq) < 3:
                continue
            out = client.search_works(qq, rows=rows)
            attempts.append({"query": qq, "item_count": len(out.get("items") or []), "error": out.get("error")})
            for it in out.get("items") or []:
                doi = _normalize_doi(it.get("doi"))
                title = (it.get("title") or "").strip()
                key = doi or _normalize_text(title)
                if not key or key in seen:
                    continue
                seen.add(key)
                merged_items.append(it)

        # lightweight type+keyword gate to reduce noisy tail
        allowed_types = {"journal-article", "posted-content", "proceedings-article"}
        topic_terms = {
            "combust",
            "kinetic",
            "chemical",
            "ode",
            "cfd",
            "flame",
            "ammonia",
            "surrogate",
            "deep learning",
            "neural",
        }

        gated = []
        for it in merged_items:
            typ = (it.get("type") or "").strip().lower()
            if typ and typ not in allowed_types:
                continue
            title = (it.get("title") or "").strip()
            if not title or len(title) > 320:
                continue
            bad_frag = ("journal vol", "page ", "view download", "authors:", "doi no:")
            ttl = title.lower()
            if any(b in ttl for b in bad_frag):
                continue

            text = f"{title} {it.get('journal') or ''}".lower()
            hit = any(t in text for t in topic_terms)
            if not hit:
                continue
            s = {
                "paper_id": f"doi:{_normalize_doi(it.get('doi'))}" if _normalize_doi(it.get("doi")) else None,
                "doi": _normalize_doi(it.get("doi")),
                "arxiv": None,
                "title": it.get("title") or "",
                "journal": it.get("journal") or "",
                "publication_date": it.get("published"),
                "relevance_score": it.get("score"),
                "sort_score": it.get("score"),
                "source": "crossref_query",
                "link": it.get("url") or (f"https://doi.org/{_normalize_doi(it.get('doi'))}" if _normalize_doi(it.get("doi")) else None),
                "crossref_type": typ,
            }
            if s["paper_id"]:
                gated.append(s)
            if len(gated) >= top_k * 2:
                break

        gated.sort(key=lambda x: float(x.get("relevance_score") or 0.0), reverse=True)
        return gated[: max(1, min(top_k * 2, 100))], {
            "error": None,
            "rows": rows,
            "attempts": attempts,
            "merged_item_count": len(merged_items),
            "gated_item_count": len(gated),
        }

    def seed_candidates_from_query_id(
        self,
        query: str,
        query_id: str,
        top_k: int = 20,
        sort: str = "RelevanceScore",
        provider: str | None = None,
        model: str | None = None,
        thinking: str | None = None,
    ) -> dict:
        query = (query or "").strip()
        if len(query) < 3:
            raise ValueError("query must be at least 3 chars")

        top_k = max(1, min(int(top_k), 200))
        llm_instruction = (
            "Rewrite the research query for scholarly search. Return strict JSON only with keys "
            "expanded_query (string), keywords (array of strings), inclusion (array), exclusion (array).\n"
            f"Query: {query}"
        )
        llm_out = self.llm_prompt(prompt=llm_instruction, provider=provider, model=model, thinking=thinking)

        expanded = query
        llm_structured = None
        if llm_out.get("ok") and llm_out.get("response"):
            try:
                llm_structured = json.loads(llm_out.get("response") or "{}")
                expanded = (llm_structured.get("expanded_query") or "").strip() or query
            except Exception:
                expanded = query

        papers = self.bohrium_question_papers(query_id=query_id, sort=sort)
        items = list(papers.get("items") or [])

        def score(it: dict) -> float:
            rs = it.get("relevance_score")
            ss = it.get("sort_score")
            try:
                if rs is not None:
                    return float(rs)
                if ss is not None:
                    return float(ss)
            except Exception:
                pass
            return 0.0

        items.sort(key=score, reverse=True)
        seeds = []
        for it in items:
            s = self._paper_item_to_seed(it)
            if not s:
                continue
            seeds.append(s)
            if len(seeds) >= top_k:
                break

        mentioned = self._mentioned_seed_candidates(query=query, items=items)
        seeds = self._merge_unique_seeds(mentioned, seeds, limit=top_k)

        return {
            "input_query": query,
            "expanded_query": expanded,
            "llm": {
                "ok": llm_out.get("ok"),
                "provider": provider,
                "model": model,
                "thinking": thinking,
                "structured": llm_structured,
                "error": llm_out.get("stderr") if not llm_out.get("ok") else None,
            },
            "query_id": str(query_id),
            "sort": sort,
            "seed_count": len(seeds),
            "seeds": seeds,
            "bohrium_meta": {
                "code": papers.get("code"),
                "source_list": papers.get("source_list"),
                "log_id": papers.get("log_id"),
                "raw_item_count": len(items),
                "error": papers.get("error"),
            },
        }

    def seed_candidates(self, query: str, top_k: int = 20, sort: str = "RelevanceScore", provider: str | None = None, model: str | None = None, thinking: str | None = None, sigma_model: str = "auto", discipline: str = "All", wait_seconds: int = 25, poll_interval: float = 1.5, min_seed_count: int = 5, crossref_rows: int = 30) -> dict:
        created = self.bohrium_create_session(query=query, model=sigma_model, discipline=discipline, resource_id_list=[])
        uid = created.get("uuid")
        if not uid:
            return {
                "input_query": query,
                "expanded_query": query,
                "llm": {"ok": False, "provider": provider, "model": model, "thinking": thinking, "structured": None, "error": "failed_to_create_bohrium_session"},
                "query_id": None,
                "sort": sort,
                "seed_count": 0,
                "seeds": [],
                "bohrium_meta": {"code": created.get("code"), "source_list": [], "log_id": None, "raw_item_count": 0, "error": created.get("error")},
                "session": created,
            }

        qid = None
        detail = None
        deadline = time.time() + max(0, int(wait_seconds))
        while True:
            detail = self.bohrium_session_detail(uuid=uid)
            qid = detail.get("query_id") if detail else None
            if qid:
                break
            if time.time() >= deadline:
                break
            time.sleep(max(0.2, float(poll_interval)))

        if not qid:
            return {
                "input_query": query,
                "expanded_query": query,
                "llm": {"ok": False, "provider": provider, "model": model, "thinking": thinking, "structured": None, "error": "query_id_not_ready"},
                "query_id": None,
                "sort": sort,
                "seed_count": 0,
                "seeds": [],
                "bohrium_meta": {"code": (detail or {}).get("code"), "source_list": [], "log_id": None, "raw_item_count": 0, "error": (detail or {}).get("error")},
                "session": created,
                "detail": detail,
            }

        out = self.seed_candidates_from_query_id(query=query, query_id=str(qid), top_k=top_k, sort=sort, provider=provider, model=model, thinking=thinking)

        # optional polling for papers when query is still processing
        while out.get("seed_count", 0) == 0 and time.time() < deadline:
            detail = self.bohrium_session_detail(uuid=uid)
            q_status = None
            if detail and detail.get("questions"):
                q_status = (detail.get("questions")[0] or {}).get("status")
            if q_status and str(q_status).lower() in ("done", "completed", "success", "finish", "finished"):
                break
            time.sleep(max(0.2, float(poll_interval)))
            out = self.seed_candidates_from_query_id(query=query, query_id=str(qid), top_k=top_k, sort=sort, provider=provider, model=model, thinking=thinking)

        # enforce minimum seed count via fallback broader queries
        target_min = max(1, min(int(min_seed_count), int(top_k)))
        fallback_attempts = []
        if out.get("seed_count", 0) < target_min:
            variants = self._fallback_query_variants(query=query, expanded_query=out.get("expanded_query") or "")
            for vq in variants:
                if out.get("seed_count", 0) >= target_min or time.time() >= deadline:
                    break
                created2 = self.bohrium_create_session(query=vq, model=sigma_model, discipline=discipline, resource_id_list=[])
                uid2 = created2.get("uuid")
                if not uid2:
                    fallback_attempts.append({"query": vq, "error": "failed_to_create_session"})
                    continue

                d2 = None
                qid2 = None
                while time.time() < deadline:
                    d2 = self.bohrium_session_detail(uuid=uid2)
                    qid2 = (d2 or {}).get("query_id")
                    if qid2:
                        break
                    time.sleep(max(0.2, float(poll_interval)))

                if not qid2:
                    fallback_attempts.append({"query": vq, "uuid": uid2, "error": "query_id_not_ready"})
                    continue

                p2 = self.bohrium_question_papers(query_id=str(qid2), sort=sort)
                items2 = list(p2.get("items") or [])
                items2.sort(key=lambda it: float(it.get("relevance_score") or it.get("sort_score") or 0.0), reverse=True)
                add = [self._paper_item_to_seed(it) for it in items2]
                add = [x for x in add if x]
                before = len(out.get("seeds") or [])
                out["seeds"] = self._merge_unique_seeds(out.get("seeds") or [], add, limit=top_k)
                out["seed_count"] = len(out.get("seeds") or [])
                fallback_attempts.append({
                    "query": vq,
                    "uuid": uid2,
                    "query_id": str(qid2),
                    "raw_item_count": len(items2),
                    "added_unique": len(out.get("seeds") or []) - before,
                    "error": p2.get("error"),
                })

        crossref_topup = None
        if out.get("seed_count", 0) < target_min:
            add_cr, crossref_topup = self._topup_seeds_from_crossref(
                query=query,
                expanded_query=out.get("expanded_query") or "",
                rows=max(5, min(int(crossref_rows), 100)),
                top_k=top_k,
            )
            before = len(out.get("seeds") or [])
            out["seeds"] = self._merge_unique_seeds(out.get("seeds") or [], add_cr, limit=top_k)
            out["seed_count"] = len(out.get("seeds") or [])
            if crossref_topup is not None:
                crossref_topup["added_unique"] = len(out.get("seeds") or []) - before

        out["session"] = created
        out["detail"] = detail
        out["min_seed_count_target"] = target_min
        out["fallback_attempts"] = fallback_attempts
        out["crossref_topup"] = crossref_topup
        return out

    def op_search(
        self,
        prompt: str,
        top_k: int = 20,
        min_seed_count: int = 5,
        crossref_rows: int = 30,
        sort: str = "RelevanceScore",
        provider: str | None = None,
        model: str | None = None,
        thinking: str | None = None,
        sigma_model: str = "auto",
        discipline: str = "ET",
        wait_seconds: int = 30,
        poll_interval: float = 1.5,
    ) -> dict:
        out = self.seed_candidates(
            query=prompt,
            top_k=top_k,
            sort=sort,
            provider=provider,
            model=model,
            thinking=thinking,
            sigma_model=sigma_model,
            discipline=discipline,
            wait_seconds=wait_seconds,
            poll_interval=poll_interval,
            min_seed_count=min_seed_count,
            crossref_rows=crossref_rows,
        )
        return {
            "operation": "search",
            "prompt": prompt,
            "new_papers": out.get("seeds") or [],
            "new_paper_count": int(out.get("seed_count") or 0),
            "query_id": out.get("query_id"),
            "search_meta": {
                "bohrium": out.get("bohrium_meta"),
                "crossref_topup": out.get("crossref_topup"),
                "fallback_attempts": out.get("fallback_attempts"),
            },
            "raw": out,
        }

    def _classify_candidate(self, topic: str, cand: dict[str, Any], provider: str | None, model: str | None, thinking: str | None) -> dict:
        title = (cand.get("title") or "").strip()
        doi = _normalize_doi(cand.get("doi"))
        abstract = (cand.get("abstract") or "").strip()
        full_text = (cand.get("full_text") or "").strip()
        journal = (cand.get("journal") or cand.get("venue") or "").strip()
        publication_date = cand.get("publication_date") or cand.get("published")

        content = full_text if full_text else abstract
        if not content:
            return {
                "paper_id": f"doi:{doi}" if doi else None,
                "doi": doi,
                "title": title,
                "journal": journal,
                "publication_date": publication_date,
                "label": "non_classifiable",
                "reason": "no_full_text_or_abstract",
                "llm_ok": False,
            }

        content = content[:8000]
        prompt = (
            "Classify a candidate paper against the research topic. Return strict JSON only with keys: "
            "label, reason. label must be one of highly_relevant, closely_related, ignorable. "
            "If uncertain, choose closely_related.\n"
            f"Topic:\n{topic}\n\n"
            f"Paper title:\n{title}\n\n"
            f"Journal/venue:\n{journal}\n\n"
            f"Publication date:\n{publication_date or ''}\n\n"
            f"Paper content (full text preferred, else abstract):\n{content}\n"
        )
        llm = self.llm_prompt(prompt=prompt, provider=provider, model=model, thinking=thinking)
        parsed = _parse_json_object(llm.get("response")) if llm.get("ok") else None
        label = str((parsed or {}).get("label") or "").strip().lower().replace(" ", "_")
        if label not in ("highly_relevant", "closely_related", "ignorable"):
            label = "non_classifiable"

        return {
            "paper_id": f"doi:{doi}" if doi else None,
            "doi": doi,
            "title": title,
            "journal": journal,
            "publication_date": publication_date,
            "label": label,
            "reason": ((parsed or {}).get("reason") or llm.get("stderr") or "").strip(),
            "llm_ok": bool(llm.get("ok")),
        }

    def _classify_candidates_batch(self, topic: str, candidates: list[dict[str, Any]], provider: str | None, model: str | None, thinking: str | None) -> tuple[list[dict], int]:
        if not candidates:
            return [], 0

        payload = []
        base = []
        for idx, cand in enumerate(candidates):
            title = (cand.get("title") or "").strip()
            doi = _normalize_doi(cand.get("doi"))
            abstract = (cand.get("abstract") or "").strip()
            full_text = (cand.get("full_text") or "").strip()
            journal = (cand.get("journal") or cand.get("venue") or "").strip()
            publication_date = cand.get("publication_date") or cand.get("published")
            content = (full_text if full_text else abstract).strip()

            base.append(
                {
                    "paper_id": f"doi:{doi}" if doi else None,
                    "doi": doi,
                    "title": title,
                    "journal": journal,
                    "publication_date": publication_date,
                }
            )

            if not content:
                payload.append({"idx": idx, "skip": True})
            else:
                payload.append(
                    {
                        "idx": idx,
                        "title": title,
                        "journal": journal,
                        "publication_date": publication_date or "",
                        "content": content[:5000],
                    }
                )

        prompt = (
            "Classify candidate papers against the research topic. Return strict JSON array only. "
            "Each element must have keys: idx, label, reason. "
            "label must be one of highly_relevant, closely_related, ignorable. "
            "If uncertain, choose closely_related.\n"
            f"Topic:\n{topic}\n\n"
            "Candidates JSON:\n"
            f"{json.dumps(payload, ensure_ascii=False)}"
        )

        llm = self.llm_prompt(prompt=prompt, provider=provider, model=model, thinking=thinking)
        if not llm.get("ok"):
            out = []
            for b, p in zip(base, payload):
                if p.get("skip"):
                    out.append({**b, "label": "non_classifiable", "reason": "no_full_text_or_abstract", "llm_ok": False})
                else:
                    out.append({**b, "label": "non_classifiable", "reason": (llm.get("stderr") or "batch_llm_failed").strip(), "llm_ok": False})
            return out, 1

        parsed = _parse_json_array(llm.get("response")) or []
        by_idx = {}
        for it in parsed:
            if not isinstance(it, dict):
                continue
            i = it.get("idx")
            try:
                i = int(i)
            except Exception:
                continue
            by_idx[i] = it

        out = []
        for i, (b, p) in enumerate(zip(base, payload)):
            if p.get("skip"):
                out.append({**b, "label": "non_classifiable", "reason": "no_full_text_or_abstract", "llm_ok": False})
                continue
            one = by_idx.get(i) or {}
            label = str(one.get("label") or "").strip().lower().replace(" ", "_")
            if label not in ("highly_relevant", "closely_related", "ignorable"):
                label = "non_classifiable"
            out.append(
                {
                    **b,
                    "label": label,
                    "reason": str(one.get("reason") or "").strip(),
                    "llm_ok": bool(by_idx.get(i)),
                }
            )

        return out, 1

    def op_classify(
        self,
        topic: str,
        candidates: list[dict[str, Any]] | None = None,
        query_id: str | None = None,
        top_k: int = 20,
        sort: str = "RelevanceScore",
        provider: str | None = DEFAULT_LLM_PROVIDER,
        model: str | None = DEFAULT_LLM_MODEL,
        thinking: str | None = DEFAULT_LLM_THINKING,
        max_workers: int = 2,
        batch_size: int = 5,
    ) -> dict:
        topic = (topic or "").strip()
        if len(topic) < 3:
            raise ValueError("topic must be at least 3 chars")

        items = list(candidates or [])
        source = "input_candidates"
        raw_meta = None
        if not items:
            if not query_id:
                raise ValueError("query_id is required when candidates is empty")
            papers = self.bohrium_question_papers(query_id=str(query_id), sort=sort)
            items = list(papers.get("items") or [])
            raw_meta = {
                "code": papers.get("code"),
                "source_list": papers.get("source_list"),
                "log_id": papers.get("log_id"),
                "error": papers.get("error"),
            }
            source = "bohrium_query_id"

        items = items[: max(1, min(int(top_k), 200))]
        max_workers = max(1, min(int(max_workers), 8))
        batch_size = max(1, min(int(batch_size), 20))
        t0 = time.perf_counter()

        results = []
        llm_calls = 0
        if batch_size <= 1:
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futs = [ex.submit(self._classify_candidate, topic, it, provider, model, thinking) for it in items]
                for fut in as_completed(futs):
                    results.append(fut.result())
            llm_calls = sum(1 for it in items if (it.get("abstract") or it.get("full_text") or "").strip())
        else:
            for i in range(0, len(items), batch_size):
                batch = items[i : i + batch_size]
                batch_out, calls = self._classify_candidates_batch(topic, batch, provider, model, thinking)
                llm_calls += calls
                results.extend(batch_out)

        order = {(it.get("doi") or it.get("title") or ""): idx for idx, it in enumerate(items)}
        results.sort(key=lambda x: order.get(x.get("doi") or x.get("title") or "", 10**9))

        counts = {
            "highly_relevant": sum(1 for x in results if x.get("label") == "highly_relevant"),
            "closely_related": sum(1 for x in results if x.get("label") == "closely_related"),
            "ignorable": sum(1 for x in results if x.get("label") == "ignorable"),
            "non_classifiable": sum(1 for x in results if x.get("label") == "non_classifiable"),
        }

        dur_ms = _ms_since(t0)
        return {
            "operation": "relevance_classification",
            "topic": topic,
            "source": source,
            "query_id": str(query_id) if query_id else None,
            "counts": counts,
            "items_classified": len(results),
            "items": results,
            "meta": {
                "upstream": raw_meta,
                "perf": {
                    "duration_ms": dur_ms,
                    "batch_size": batch_size,
                    "llm_calls": llm_calls,
                    "items": len(items),
                    "items_per_sec": round(len(items) / max((dur_ms / 1000.0), 1e-6), 4),
                },
            },
        }

    def op_grow(
        self,
        seeds: list[str],
        levels: int = 2,
        limit_per_node: int = 30,
        use_mock: bool = False,
    ) -> dict:
        if not seeds:
            raise ValueError("seeds is required")
        levels = max(1, min(int(levels), 2))
        limit_per_node = max(1, min(int(limit_per_node), 200))

        resolved = []
        ingest = []
        for s in seeds:
            x = (s or "").strip()
            if not x:
                continue
            doi = _normalize_doi(x[4:]) if x.lower().startswith("doi:") else (_normalize_doi(x) if x.startswith("10.") else None)
            if doi:
                existing = self.repo.get_api_paper_by_doi(doi)
                if existing is None:
                    try:
                        ingest.append(self.graph_ingest_doi(doi=doi, use_mock=use_mock))
                    except Exception as e:
                        ingest.append({"doi": doi, "error": str(e)})
                else:
                    ingest.append({"doi": doi, "status": "already_present"})
                try:
                    resolved.append(self._resolve_seed(doi))
                except Exception:
                    pass
            else:
                try:
                    resolved.append(self._resolve_seed(x))
                except Exception:
                    pass

        resolved = list(dict.fromkeys(resolved))
        if not resolved:
            raise ValueError("no resolvable seeds")

        visited = set(resolved)
        frontier = list(resolved)
        level_out = []
        for lv in range(1, levels + 1):
            next_frontier = []
            edges = []
            for pid in frontier:
                neigh = self.repo.graph_neighbors(pid, direction="both", limit=limit_per_node)
                for r in neigh.get("out", []):
                    nid = r.get("paper_id")
                    if not nid:
                        continue
                    edges.append({"level": lv, "relation": "references", "src": pid, "dst": nid})
                    if nid not in visited:
                        visited.add(nid)
                        next_frontier.append(nid)
                for r in neigh.get("in", []):
                    nid = r.get("paper_id")
                    if not nid:
                        continue
                    edges.append({"level": lv, "relation": "cited_by", "src": nid, "dst": pid})
                    if nid not in visited:
                        visited.add(nid)
                        next_frontier.append(nid)

            meta_rows = self.repo.get_api_papers_by_ids(next_frontier)
            level_out.append(
                {
                    "level": lv,
                    "frontier_count": len(frontier),
                    "discovered_count": len(next_frontier),
                    "edges": edges,
                    "papers": meta_rows,
                }
            )
            frontier = next_frontier
            if not frontier:
                break

        return {
            "operation": "grow",
            "levels": levels,
            "seed_papers": resolved,
            "ingest": ingest,
            "results": level_out,
            "total_discovered_unique": len(visited),
        }

    def run_start(self, query: str) -> dict:
        meta = self.run_manager.start_run(query=query)
        self.run_manager.append_event(run_id=meta["run_id"], op="run_start", status="ok", input_payload={"query": query})
        return meta

    def run_search(self, run_id: str, **kwargs) -> dict:
        t0 = time.perf_counter()
        meta = self.run_manager.read_json(run_id, "meta.json")
        prompt = kwargs.pop("prompt", None) or meta.get("query") or ""
        crossref_rows = int(kwargs.get("crossref_rows", 30))
        out = self.op_search(prompt=prompt, **kwargs)

        # Always include crossref results in run-level search pool/output (not only top-up fallback)
        add_cr, cr_meta = self._topup_seeds_from_crossref(
            query=prompt,
            expanded_query=((out.get("raw") or {}).get("expanded_query") or ""),
            rows=max(5, min(crossref_rows, 100)),
            top_k=max(int(out.get("new_paper_count") or 0), int(kwargs.get("top_k", 20))),
        )
        merged = self._merge_unique_seeds(out.get("new_papers") or [], add_cr, limit=500)
        out["new_papers"] = merged
        out["new_paper_count"] = len(merged)
        out.setdefault("search_meta", {})["crossref_always"] = cr_meta

        self.run_manager.write_json(run_id, "search.json", out)
        pool_stats = self.run_manager.upsert_pool(run_id, papers=out.get("new_papers") or [], source_op="search")
        self.run_manager.append_event(
            run_id=run_id,
            op="search",
            status="ok",
            input_payload={"prompt": prompt, **kwargs},
            output_file="search.json",
            summary=f"new_paper_count={out.get('new_paper_count', 0)} pool_size={pool_stats.get('pool_size', 0)}",
            meta={
                "pool": pool_stats,
                "perf": {
                    "duration_ms": _ms_since(t0),
                    "new_paper_count": int(out.get("new_paper_count") or 0),
                    "crossref_rows": crossref_rows,
                },
            },
        )
        return out

    def run_classify(self, run_id: str, **kwargs) -> dict:
        t0 = time.perf_counter()
        meta = self.run_manager.read_json(run_id, "meta.json")
        topic = kwargs.pop("topic", None) or meta.get("query") or ""
        query_id = kwargs.pop("query_id", None)
        candidates = kwargs.pop("candidates", None)

        # Default behavior: classify the persistent run pool first (not a fresh query_id fetch)
        if candidates is None:
            pool_rows = self.run_manager.list_pool_papers(run_id)
            if pool_rows:
                def _pool_score(x: dict) -> tuple:
                    has_abs = 1 if (x.get("abstract") or x.get("full_text") or "").strip() else 0
                    src = (x.get("source") or "").lower()
                    src_boost = 2 if src == "bohrium" else (1 if src == "crossref_query" else 0)
                    rel = float(x.get("relevance_score") or x.get("sort_score") or 0.0)
                    return (has_abs, src_boost, rel)

                pool_rows.sort(key=_pool_score, reverse=True)
                top_k = max(1, min(int(kwargs.get("top_k", 20)), 200))
                candidates = pool_rows[:top_k]

        if query_id is None and candidates is None:
            search = self.run_manager.read_json(run_id, "search.json")
            query_id = search.get("query_id")

        enrich_abstract_max = int(kwargs.pop("enrich_abstract_max", 20))
        enrich_workers = int(kwargs.pop("enrich_workers", 4))
        enrich_stats = {
            "attempted": 0,
            "before_with_abstract": 0,
            "after_with_abstract": 0,
            "duration_ms": 0,
        }
        if candidates is not None:
            enrich_stats["attempted"] = len(candidates)
            enrich_stats["before_with_abstract"] = sum(1 for x in candidates if (x.get("abstract") or x.get("full_text") or "").strip())
            t_enrich = time.perf_counter()
            candidates = self._enrich_candidates_with_abstracts(
                candidates,
                max_fetch=enrich_abstract_max,
                max_workers=enrich_workers,
            )
            enrich_stats["duration_ms"] = _ms_since(t_enrich)
            enrich_stats["after_with_abstract"] = sum(1 for x in candidates if (x.get("abstract") or x.get("full_text") or "").strip())
            self.run_manager.upsert_pool(run_id, papers=candidates, source_op="classify_enrich")

        out = self.op_classify(topic=topic, query_id=query_id, candidates=candidates, **kwargs)
        self.run_manager.write_json(run_id, "classify.json", out)
        self.run_manager.append_event(
            run_id=run_id,
            op="classify",
            status="ok",
            input_payload={"topic": topic, "query_id": query_id, "used_pool_candidates": candidates is not None, **kwargs},
            output_file="classify.json",
            summary=f"items_classified={out.get('items_classified', 0)}",
            meta={
                "perf": {
                    "duration_ms": _ms_since(t0),
                    "items_classified": int(out.get("items_classified") or 0),
                    "non_classifiable": int((out.get("counts") or {}).get("non_classifiable") or 0),
                    "enrich": enrich_stats,
                }
            },
        )
        return out

    def _enrich_candidates_with_abstracts(self, candidates: list[dict[str, Any]], max_fetch: int = 20, max_workers: int = 4) -> list[dict[str, Any]]:
        if not candidates:
            return []
        max_fetch = max(1, min(int(max_fetch), 100))
        max_workers = max(1, min(int(max_workers), 8))
        out = [dict(c) for c in candidates]

        todo: list[tuple[int, str]] = []
        for i, c in enumerate(out):
            if i >= max_fetch:
                break
            if (c.get("abstract") or c.get("full_text") or "").strip():
                continue
            d = _normalize_doi(c.get("doi"))
            if d:
                todo.append((i, d))

        if not todo:
            return out

        oa = OpenAlexClient()
        ev = ElsevierFullTextClient()

        def _fetch_one(doi: str) -> dict[str, Any]:
            update: dict[str, Any] = {}

            # 1) Elsevier first for likely-valid DOI patterns (best hit rate in our domain).
            if _is_likely_doi(doi):
                xml, meta = ev.fetch_xml_by_doi(doi, use_mock=False)
                abs_text = ""
                if xml:
                    try:
                        abs_text = (extract_abstract(ET.fromstring(xml)) or "").strip()
                    except Exception:
                        abs_text = ""
                if abs_text:
                    update["abstract"] = abs_text
                    update["abstract_source"] = "elsevier"
                    update["abstract_status"] = "ok"
                    return update
                if meta.get("status") in (400, 401, 403):
                    update["abstract_status"] = "entitlement_or_input_blocked"

            # 2) OpenAlex fallback.
            w = oa.work_by_doi(doi)
            abs_text = (w.get("abstract") or "").strip()
            if abs_text:
                update["abstract"] = abs_text
                update["abstract_source"] = "openalex"
                update["abstract_status"] = "ok"
            elif not update.get("abstract_status"):
                update["abstract_status"] = "missing"

            if not update.get("abstract") and w.get("error"):
                update["abstract_error"] = str(w.get("error"))[:300]

            if not update.get("abstract"):
                # keep existing metadata fields untouched if no abstract enrichment happened
                return update

            if (w.get("venue") or "").strip():
                update["journal"] = w.get("venue")
            if w.get("year"):
                update["publication_date"] = f"{w.get('year')}-01-01"
            return update

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = {ex.submit(_fetch_one, doi): (idx, doi) for idx, doi in todo}
            for fut in as_completed(futs):
                idx, _doi = futs[fut]
                try:
                    patch = fut.result() or {}
                except Exception:
                    continue
                if not patch:
                    continue
                for k, v in patch.items():
                    if k in ("journal", "publication_date"):
                        if not out[idx].get(k) and v:
                            out[idx][k] = v
                    elif v is not None:
                        out[idx][k] = v

        return out

    def _run_seed_dois(self, run_id: str, fallback_max: int = 5) -> list[str]:
        classify = self.run_manager._try_read(run_id, "classify.json")
        seeds: list[str] = []
        if classify:
            for it in classify.get("items", []) or []:
                if it.get("label") in ("highly_relevant", "closely_related"):
                    d = _normalize_doi(it.get("doi"))
                    if d:
                        seeds.append(d)
        if not seeds:
            search = self.run_manager._try_read(run_id, "search.json") or {}
            papers = list(search.get("new_papers", []) or [])
            papers.sort(key=lambda x: float(x.get("relevance_score") or x.get("sort_score") or 0.0), reverse=True)
            for p in papers[: max(1, min(int(fallback_max), 20))]:
                d = _normalize_doi(p.get("doi"))
                if d:
                    seeds.append(d)
        return list(dict.fromkeys(seeds))

    def _run_search_pool_dois(self, run_id: str, max_count: int | None = None) -> list[str]:
        search = self.run_manager._try_read(run_id, "search.json") or {}
        papers = list(search.get("new_papers", []) or [])
        dois: list[str] = []
        for p in papers:
            d = _normalize_doi(p.get("doi"))
            if d:
                dois.append(d)
        uniq = list(dict.fromkeys(dois))
        if max_count is None:
            return uniq
        return uniq[: max(1, min(int(max_count), len(uniq) if uniq else 1))]

    def run_grow(
        self,
        run_id: str,
        levels: int = 2,
        limit_per_node: int = 30,
        use_mock: bool = False,
        fallback_seed_max: int = 5,
        seed_strategy: str = "search_pool",
        search_seed_max: int | None = None,
    ) -> dict:
        t0 = time.perf_counter()
        if seed_strategy == "search_pool":
            seeds = self._run_search_pool_dois(run_id, max_count=search_seed_max)
            if not seeds:
                seeds = self._run_seed_dois(run_id, fallback_max=fallback_seed_max)
        else:
            seeds = self._run_seed_dois(run_id, fallback_max=fallback_seed_max)
        out = self.op_grow(seeds=seeds, levels=levels, limit_per_node=limit_per_node, use_mock=use_mock)
        self.run_manager.write_json(run_id, "grow.json", out)

        grow_papers = []
        for lv in out.get("results", []) or []:
            for p in lv.get("papers", []) or []:
                grow_papers.append(
                    {
                        "paper_id": p.get("paper_id"),
                        "doi": p.get("doi"),
                        "title": p.get("title"),
                        "journal": p.get("venue"),
                        "publication_date": (f"{p.get('year')}-01-01" if p.get("year") else None),
                        "source": p.get("source") or "grow",
                        "abstract": "",
                    }
                )
        pool_stats = self.run_manager.upsert_pool(run_id, papers=grow_papers, source_op="grow")

        self.run_manager.append_event(
            run_id=run_id,
            op="grow",
            status="ok",
            input_payload={"seeds": seeds, "levels": levels, "limit_per_node": limit_per_node},
            output_file="grow.json",
            summary=f"total_discovered_unique={out.get('total_discovered_unique', 0)} pool_size={pool_stats.get('pool_size', 0)}",
            meta={
                "pool": pool_stats,
                "perf": {
                    "duration_ms": _ms_since(t0),
                    "seed_count": len(seeds),
                    "levels": levels,
                    "limit_per_node": limit_per_node,
                    "total_discovered_unique": int(out.get("total_discovered_unique") or 0),
                },
            },
        )
        return out

    def _rank_within_run_pool(
        self,
        run_id: str,
        limit: int = 20,
        alpha: float = 0.85,
        max_iter: int = 100,
        tol: float = 1e-7,
        pool_bias_strength: float = 0.8,
        seed_init_boost: float = 3.0,
        seed_pool_factor_floor: float = 0.06,
        seed_rescue_max: int = 8,
        anchor_score_boost: float = 4.0,
    ) -> dict:
        pool_rows = self.run_manager.list_pool_papers(run_id)
        node_ids = [str((r.get("paper_id") or "")).strip().lower() for r in pool_rows if (r.get("paper_id") or "").strip()]
        node_ids = list(dict.fromkeys(node_ids))
        if not node_ids:
            return {"operation": "rank", "mode": "pool", "items": [], "reason": "empty_pool"}

        node_set = set(node_ids)
        pool_map = {str((r.get("paper_id") or "")).strip().lower(): r for r in pool_rows}

        edges = self.repo.get_all_edges()
        out_adj: dict[str, list[str]] = {n: [] for n in node_ids}
        in_adj: dict[str, list[str]] = {n: [] for n in node_ids}
        for src, dst in edges:
            s = str(src).strip().lower()
            d = str(dst).strip().lower()
            if s in node_set and d in node_set:
                out_adj[s].append(d)
                in_adj[d].append(s)

        meta_rows = self.repo.get_api_papers_by_ids(node_ids)
        meta_map = {str(r.get("paper_id") or "").strip().lower(): r for r in meta_rows}

        # Seed-biased initialization/personalization: boost initial search papers.
        search = self.run_manager._try_read(run_id, "search.json") or {}
        seed_ids: set[str] = set()
        for p in (search.get("new_papers") or []):
            pid = (p.get("paper_id") or "").strip().lower()
            if pid and pid in node_set:
                seed_ids.add(pid)
                continue
            d = _normalize_doi(p.get("doi"))
            if d:
                pid2 = f"doi:{d}"
                if pid2 in node_set:
                    seed_ids.add(pid2)

        # On-the-fly seed rescue: if a seed has zero pool links and zero global counts,
        # try live DOI ingest once to populate metadata/references/edges.
        seed_rescue_max = max(0, min(int(seed_rescue_max), 20))
        rescued = 0
        if seed_rescue_max > 0 and seed_ids:
            oa = OpenAlexClient()
            cr = CrossrefClient()
            now = _now_iso()
            for sid in list(seed_ids):
                if rescued >= seed_rescue_max:
                    break
                m = meta_map.get(sid, {})
                in_pool = len(in_adj.get(sid, []))
                out_pool = len(out_adj.get(sid, []))
                in_g = int(_safe_float(m.get("citation_count"), 0.0))
                out_g = int(_safe_float(m.get("reference_count"), 0.0))
                if (in_pool + out_pool) > 0 or in_g > 0 or out_g > 0:
                    continue

                p = pool_map.get(sid, {})
                d = _normalize_doi(m.get("doi")) or _normalize_doi(p.get("doi"))
                if not d and sid.startswith("doi:"):
                    d = _normalize_doi(sid[4:])
                if not d:
                    continue
                d_lookup = self._resolve_arxiv_published_doi(d) or d

                # 1) Quick metadata + count refresh from APIs.
                try:
                    ow = oa.work_by_doi(d_lookup)
                except Exception:
                    ow = {}
                try:
                    cw = cr.references_by_doi(d_lookup)
                except Exception:
                    cw = {}

                cite = _pick_count_primary_fallback(ow.get("citation_count"), cw.get("citation_count"))
                ref = _pick_count_primary_fallback(ow.get("reference_count"), cw.get("reference_count"))
                title = (ow.get("title") or cw.get("title") or p.get("title") or m.get("title") or d or "").strip()
                venue = (ow.get("venue") or cw.get("venue") or p.get("journal") or m.get("venue") or "").strip()
                year = _year_from_any(ow.get("year")) or _year_from_any(cw.get("year")) or _year_from_any((p.get("publication_date") or "")[:4]) or _year_from_any(m.get("year"))
                source = "openalex" if (ow.get("title") or "").strip() else ("crossref" if (cw.get("title") or "").strip() else (p.get("source") or m.get("source") or "seed_rescue"))

                try:
                    self.repo.upsert_api_paper(
                        {
                            "paper_id": sid,
                            "doi": d,
                            "openalex_id": None,
                            "citation_count": cite,
                            "reference_count": ref,
                            "title": title or d,
                            "year": year,
                            "venue": venue,
                            "abstract": (ow.get("abstract") or "").strip(),
                            "source": source,
                            "updated_at": now,
                        }
                    )
                except Exception:
                    pass

                # 2) If references likely exist, try full ingest to materialize edges.
                try:
                    if ref > 0:
                        self.graph_ingest_doi(doi=d, use_mock=False)
                    rescued += 1
                except Exception:
                    continue

            if rescued > 0:
                edges = self.repo.get_all_edges()
                out_adj = {n: [] for n in node_ids}
                in_adj = {n: [] for n in node_ids}
                for src, dst in edges:
                    s = str(src).strip().lower()
                    d = str(dst).strip().lower()
                    if s in node_set and d in node_set:
                        out_adj[s].append(d)
                        in_adj[d].append(s)
                meta_rows = self.repo.get_api_papers_by_ids(node_ids)
                meta_map = {str(r.get("paper_id") or "").strip().lower(): r for r in meta_rows}

        # Anchor paper boost: if query includes a specific DOI, preserve it across rounds.
        run_meta = self.run_manager._try_read(run_id, "meta.json") or {}
        anchor_doi = _normalize_doi(_extract_doi(run_meta.get("query") or ""))
        anchor_pid = f"doi:{anchor_doi}" if anchor_doi else None
        anchor_score_boost = max(1.0, min(float(anchor_score_boost), 10.0))

        boost = max(1.0, float(seed_init_boost))
        init_raw: dict[str, float] = {}
        for pid in node_ids:
            init_raw[pid] = boost if pid in seed_ids else 1.0
        z = sum(init_raw.values()) or float(len(node_ids))

        pers = {k: (init_raw[k] / z) for k in node_ids}
        pr = {k: (init_raw[k] / z) for k in node_ids}
        converged = False
        iters = 0

        for i in range(max_iter):
            iters = i + 1
            new_pr = {k: (1.0 - alpha) * pers[k] for k in node_ids}
            dangling_mass = sum(pr[k] for k in node_ids if not out_adj[k])
            for k in node_ids:
                new_pr[k] += alpha * dangling_mass * pers[k]

            for src in node_ids:
                outs = out_adj[src]
                if not outs:
                    continue
                share = alpha * pr[src] / float(len(outs))
                for dst in outs:
                    new_pr[dst] += share

            delta = sum(abs(new_pr[k] - pr[k]) for k in node_ids)
            pr = new_pr
            if delta < tol:
                converged = True
                break

        # Pool row metadata fallback improves visibility for non-ingested nodes.

        # Anti-noise / quality priors (inspired by robust citation-ranking literature).
        pool_bias_strength = max(0.0, min(float(pool_bias_strength), 1.0))
        cred_scale = 50.0
        support_scale = 12.0
        recency_half_life = 20.0

        items = []
        for pid in node_ids:
            m = meta_map.get(pid, {})
            p = pool_map.get(pid, {})

            doi = m.get("doi") or p.get("doi")
            title = m.get("title") or p.get("title") or pid
            year = m.get("year") if m.get("year") is not None else _year_from_any((p.get("publication_date") or "")[:4])
            venue = m.get("venue") or p.get("journal") or ""
            source = m.get("source") or p.get("source") or "fallback"

            in_pool = len(in_adj.get(pid, []))
            out_pool = len(out_adj.get(pid, []))
            in_g = int(_safe_float(m.get("citation_count"), 0.0))
            out_g = int(_safe_float(m.get("reference_count"), 0.0))

            # Pool-vs-global shares (strict global semantics preserved).
            cite_score = (float(in_pool) / float(max(in_g, 1))) if in_g > 0 else 0.0
            ref_score = (float(out_pool) / float(max(out_g, 1))) if out_g > 0 else 0.0
            pool_focus = 0.6 * cite_score + 0.4 * ref_score
            # Non-linear pool bias: strongly suppress tiny in-pool/global overlap.
            pool_focus_sqrt = math.sqrt(max(pool_focus, 0.0))
            pool_factor = (1.0 - pool_bias_strength) * pool_focus_sqrt + pool_bias_strength * pool_focus
            if pid in seed_ids:
                pool_factor = max(pool_factor, max(0.0, min(float(seed_pool_factor_floor), 0.5)))

            # Reliability: very small global count totals are noisy and overconfident.
            cred_raw = 1.0 - math.exp(-float(max(in_g, 0) + max(out_g, 0)) / cred_scale)
            credibility = 0.2 + 0.8 * cred_raw

            # Local support: reward papers that are substantively connected inside this run pool.
            support_raw = 1.0 - math.exp(-float(max(in_pool, 0) + max(out_pool, 0)) / support_scale)
            support = 0.3 + 0.7 * support_raw

            # Structural balance: penalize one-sided hub artifacts (e.g., cited-only classics / dangling noise).
            ssum = float(in_pool + out_pool)
            if ssum <= 0.0:
                balance = 0.0
            else:
                balance = (2.0 * float(in_pool) * float(out_pool)) / (ssum * ssum)
            structure = 0.4 + 0.6 * balance

            # Mild recency prior to reduce old-paper dominance (can be neutral if year missing).
            y = _year_from_any(year)
            cur_year = datetime.now(timezone.utc).year
            age = float(max(0, cur_year - y)) if y is not None else recency_half_life
            recency = max(0.45, math.exp(-age / recency_half_life))

            # Metadata quality prior: down-weight fallback/low-quality records.
            quality = 1.0
            if str(source).strip().lower() == "fallback":
                quality *= 0.65
            if y is None:
                quality *= 0.85
            tnorm = str(title or "").strip().lower()
            if not tnorm or _is_likely_doi(tnorm):
                quality *= 0.85

            pr_value = float(pr.get(pid) or 0.0)
            adjusted = pr_value * pool_factor * credibility * support * structure * recency * quality
            is_anchor = bool(anchor_pid and pid == anchor_pid)
            if is_anchor:
                adjusted *= anchor_score_boost

            items.append(
                {
                    "paper_id": pid,
                    "doi": doi,
                    "title": title,
                    "year": year,
                    "venue": venue,
                    "source": source,
                    "citation_count": m.get("citation_count"),
                    "reference_count": m.get("reference_count"),
                    "score": round(float(adjusted), 8),
                    "pagerank": round(float(pr_value), 8),
                    "cite_in_pool": in_pool,
                    "cite_global": in_g,
                    "ref_in_pool": out_pool,
                    "ref_global": out_g,
                    "cite_score": round(float(cite_score), 8),
                    "ref_score": round(float(ref_score), 8),
                    "pool_focus": round(float(pool_focus), 8),
                    "pool_factor": round(float(pool_factor), 8),
                    "credibility": round(float(credibility), 8),
                    "support": round(float(support), 8),
                    "structure": round(float(structure), 8),
                    "recency": round(float(recency), 8),
                    "quality": round(float(quality), 8),
                    "is_anchor": is_anchor,
                    "anchor_score_boost": (anchor_score_boost if is_anchor else 1.0),
                }
            )
        items.sort(key=lambda x: float(x.get("score") or 0.0), reverse=True)

        # If query anchors a specific DOI, keep that anchor at the top for paper-centric runs.
        if anchor_pid:
            anchor_idx = next((i for i, it in enumerate(items) if str(it.get("paper_id") or "").strip().lower() == anchor_pid), None)
            if anchor_idx is not None and anchor_idx > 0 and items:
                top_score = float(items[0].get("score") or 0.0)
                anchor_item = dict(items[anchor_idx])
                anchor_item["score"] = round(top_score + 1e-8, 8)
                items = [anchor_item] + items[:anchor_idx] + items[anchor_idx + 1 :]

        total_ranked = len(items)
        for i, it in enumerate(items, start=1):
            it["rank"] = i
            it["rank_total"] = total_ranked
            it["rank_over_total"] = f"{i}/{total_ranked}"

        return {
            "operation": "rank",
            "mode": "pool",
            "pool_size": len(node_ids),
            "iterations": iters,
            "converged": converged,
            "params": {
                "limit": max(1, min(int(limit), 5000)),
                "alpha": alpha,
                "max_iter": max_iter,
                "tol": tol,
                "pool_bias_strength": pool_bias_strength,
                "seed_init_boost": boost,
                "seed_count": len(seed_ids),
                "seed_pool_factor_floor": seed_pool_factor_floor,
                "seed_rescue_max": seed_rescue_max,
                "seed_rescued": rescued,
                "anchor_doi": anchor_doi,
                "anchor_score_boost": anchor_score_boost,
            },
            "items": items[: max(1, min(int(limit), 5000))],
        }

    def run_rank(
        self,
        run_id: str,
        limit: int = 20,
        fallback_seed_max: int = 5,
        mode: str = "pool",
        pool_bias_strength: float = 0.8,
        seed_init_boost: float = 3.0,
        seed_pool_factor_floor: float = 0.06,
        seed_rescue_max: int = 8,
        anchor_score_boost: float = 4.0,
    ) -> dict:
        t0 = time.perf_counter()
        if mode == "pool":
            out = self._rank_within_run_pool(
                run_id=run_id,
                limit=limit,
                pool_bias_strength=pool_bias_strength,
                seed_init_boost=seed_init_boost,
                seed_pool_factor_floor=seed_pool_factor_floor,
                seed_rescue_max=seed_rescue_max,
                anchor_score_boost=anchor_score_boost,
            )
            seeds = []
        else:
            seeds = self._run_seed_dois(run_id, fallback_max=fallback_seed_max)
            for d in seeds[:8]:
                try:
                    if self.repo.get_api_paper_by_doi(d) is None:
                        self.graph_ingest_doi(doi=d, use_mock=False)
                except Exception:
                    pass

            if not seeds:
                out = {"operation": "rank", "seeds": [], "items": [], "reason": "no_seed_doi"}
            else:
                out = self.graph_rank(seeds=seeds, limit=limit, include_seeds=False)
                out["operation"] = "rank"

        self.run_manager.write_json(run_id, "rank.json", out)
        self.run_manager.append_event(
            run_id=run_id,
            op="rank",
            status="ok",
            input_payload={
                "seeds": seeds,
                "limit": limit,
                "mode": mode,
                "pool_bias_strength": pool_bias_strength,
                "seed_init_boost": seed_init_boost,
                "seed_pool_factor_floor": seed_pool_factor_floor,
                "seed_rescue_max": seed_rescue_max,
                "anchor_score_boost": anchor_score_boost,
            },
            output_file="rank.json",
            summary=f"rank_items={len(out.get('items') or [])}",
            meta={
                "perf": {
                    "duration_ms": _ms_since(t0),
                    "seed_count": len(seeds),
                    "rank_items": len(out.get("items") or []),
                }
            },
        )
        return out

    def run_score(self, run_id: str) -> dict:
        t0 = time.perf_counter()
        pool_rows = self.run_manager.list_pool_papers(run_id)
        classify = self.run_manager._try_read(run_id, "classify.json") or {}
        rank = self.run_manager._try_read(run_id, "rank.json") or {}

        def _pid_from_row(x: dict) -> str:
            d = _normalize_doi(x.get("doi"))
            pid = (x.get("paper_id") or "").strip().lower()
            if pid:
                return pid
            if d:
                return f"doi:{d}"
            t = (x.get("title") or "").strip().lower()
            return f"title:{hashlib.sha256(t.encode('utf-8')).hexdigest()[:16]}" if t else ""

        cls_by_pid: dict[str, dict] = {}
        cls_by_doi: dict[str, dict] = {}
        cls_by_title: dict[str, dict] = {}
        for it in classify.get("items", []) or []:
            pid = _pid_from_row(it)
            doi = _normalize_doi(it.get("doi"))
            title = _normalize_text(it.get("title"))
            if pid:
                cls_by_pid[pid] = it
            if doi:
                cls_by_doi[doi] = it
            if title:
                cls_by_title[title] = it

        pr_by_pid: dict[str, dict] = {}
        pr_by_doi: dict[str, dict] = {}
        for it in rank.get("items", []) or []:
            pid = _pid_from_row(it)
            doi = _normalize_doi(it.get("doi"))
            if pid:
                pr_by_pid[pid] = it
            if doi:
                pr_by_doi[doi] = it

        paper_ids = [str((r.get("paper_id") or "")).strip().lower() for r in pool_rows if (r.get("paper_id") or "").strip()]
        meta_rows = self.repo.get_api_papers_by_ids(list(dict.fromkeys(paper_ids)))
        meta_map = {str(m.get("paper_id") or "").strip().lower(): m for m in meta_rows}

        rel_map = {
            "highly_relevant": 1.0,
            "closely_related": 0.7,
            "ignorable": 0.1,
            "non_classifiable": 0.0,
        }

        raw_rows: list[dict] = []
        for p in pool_rows:
            pid = _pid_from_row(p)
            doi = _normalize_doi(p.get("doi"))
            title_key = _normalize_text(p.get("title"))

            c = cls_by_pid.get(pid) or (cls_by_doi.get(doi) if doi else None) or (cls_by_title.get(title_key) if title_key else None)
            r = pr_by_pid.get(pid) or (pr_by_doi.get(doi) if doi else None)
            m = meta_map.get(pid, {})

            label = (c or {}).get("label") or "non_classifiable"
            rel_score = rel_map.get(label, 0.0)
            pr_score_raw = _safe_float((r or {}).get("score"), 0.0)
            citation_count = int(_safe_float(m.get("citation_count"), 0.0)) if m.get("citation_count") is not None else 0
            cite_log = math.log1p(max(citation_count, 0))

            y = _year_from_any(m.get("year")) or _year_from_any(p.get("publication_date")) or _year_from_any((c or {}).get("publication_date"))

            raw_rows.append(
                {
                    "paper_id": pid,
                    "doi": doi,
                    "title": p.get("title") or (m.get("title") if m else "") or "",
                    "journal": p.get("journal") or (m.get("venue") if m else "") or "",
                    "publication_date": p.get("publication_date") or (c or {}).get("publication_date"),
                    "year": y,
                    "citation_count": citation_count,
                    "pagerank_score": pr_score_raw,
                    "classification_label": label,
                    "classification_reason": (c or {}).get("reason") or "",
                    "relevance_score": rel_score,
                    "source": p.get("source") or "",
                    "seen_in_ops": p.get("seen_in_ops") or [],
                    "_cite_log": cite_log,
                }
            )

        def _norm(values: list[float], fallback: float = 0.0) -> dict[float, float]:
            if not values:
                return {}
            lo, hi = min(values), max(values)
            if hi <= lo:
                return {v: fallback for v in values}
            return {v: (v - lo) / (hi - lo) for v in values}

        pr_norm_map = _norm([r["pagerank_score"] for r in raw_rows], fallback=0.0)
        cite_norm_map = _norm([r["_cite_log"] for r in raw_rows], fallback=0.0)
        year_vals = [float(r["year"]) for r in raw_rows if r.get("year") is not None]
        year_norm_map = _norm(year_vals, fallback=0.5)

        rows: list[dict] = []
        for r in raw_rows:
            year_norm = 0.5
            if r.get("year") is not None and year_norm_map:
                year_norm = year_norm_map.get(float(r["year"]), 0.5)

            score = (
                0.4 * r["relevance_score"]
                + 0.3 * pr_norm_map.get(r["pagerank_score"], 0.0)
                + 0.2 * cite_norm_map.get(r["_cite_log"], 0.0)
                + 0.1 * year_norm
            )

            row = {
                "paper_id": r["paper_id"],
                "doi": r["doi"],
                "title": r["title"],
                "journal": r["journal"],
                "publication_date": r["publication_date"],
                "year": r["year"],
                "citation_count": r["citation_count"],
                "pagerank_score": round(r["pagerank_score"], 8),
                "classification_label": r["classification_label"],
                "classification_reason": r["classification_reason"],
                "component_scores": {
                    "relevance": round(r["relevance_score"], 4),
                    "pagerank_norm": round(pr_norm_map.get(r["pagerank_score"], 0.0), 4),
                    "citation_norm": round(cite_norm_map.get(r["_cite_log"], 0.0), 4),
                    "year_norm": round(year_norm, 4),
                },
                "influence_score": round(score, 6),
                "source": r["source"],
                "seen_in_ops": r["seen_in_ops"],
            }
            rows.append(row)

        rows.sort(key=lambda x: (x.get("influence_score") or 0.0), reverse=True)

        out = {
            "run_id": run_id,
            "weights": {"relevance": 0.4, "pagerank_norm": 0.3, "citation_norm": 0.2, "year_norm": 0.1},
            "count": len(rows),
            "items": rows,
        }
        self.run_manager.write_json(run_id, "pool_scored.json", out)
        self.run_manager.append_event(
            run_id=run_id,
            op="score",
            status="ok",
            input_payload={"weights": out["weights"]},
            output_file="pool_scored.json",
            summary=f"scored_items={len(rows)}",
            meta={"perf": {"duration_ms": _ms_since(t0), "scored_items": len(rows)}},
        )
        return out

    def run_diagnostics(self, run_id: str) -> dict:
        history_path = self.run_manager.run_dir(run_id) / "history.jsonl"
        events = []
        if history_path.exists():
            for line in history_path.read_text(encoding="utf-8", errors="ignore").splitlines():
                s = line.strip()
                if not s:
                    continue
                try:
                    events.append(json.loads(s))
                except Exception:
                    continue

        stage_ms: dict[str, int] = {}
        for e in events:
            op = (e.get("op") or "").strip()
            perf = ((e.get("meta") or {}).get("perf") or {})
            ms = int(_safe_float(perf.get("duration_ms"), 0.0))
            if op and ms > 0:
                stage_ms[op] = stage_ms.get(op, 0) + ms

        ordered = sorted(stage_ms.items(), key=lambda x: x[1], reverse=True)
        top = [{"op": op, "duration_ms": ms} for op, ms in ordered[:5]]

        classify = self.run_manager._try_read(run_id, "classify.json") or {}
        grow = self.run_manager._try_read(run_id, "grow.json") or {}
        rank = self.run_manager._try_read(run_id, "rank.json") or {}

        suggestions: list[str] = []
        if ordered:
            top_op, _top_ms = ordered[0]
            suggestions.append(f"Primary bottleneck currently appears to be '{top_op}'.")

        non_cls = int((classify.get("counts") or {}).get("non_classifiable") or 0)
        cls_n = int(classify.get("items_classified") or 0)
        if cls_n > 0 and (non_cls / cls_n) >= 0.3:
            suggestions.append("High non_classifiable rate: prioritize abstract-available candidates and lower classify top_k in fast mode.")

        grow_levels = len((grow.get("results") or []))
        grow_discovered = int(grow.get("total_discovered_unique") or 0)
        if grow_discovered > 500 or grow_levels >= 2:
            suggestions.append("Grow is large: reduce limit_per_node or fallback_seed_max; consider 1-hop for fast exploratory runs.")

        rank_items = len((rank.get("items") or []))
        if rank_items >= 50:
            suggestions.append("Rank result set is wide: lower rank limit for faster report cycles.")

        if not suggestions:
            suggestions.append("No obvious single bottleneck found. Collect more runs and compare stage durations across runs.")

        out = {
            "run_id": run_id,
            "stage_durations_ms": stage_ms,
            "total_tracked_ms": int(sum(stage_ms.values())),
            "top_bottlenecks": top,
            "speedup_suggestions": suggestions,
            "inputs": {
                "classify_items": cls_n,
                "classify_non_classifiable": non_cls,
                "grow_levels": grow_levels,
                "grow_total_discovered_unique": grow_discovered,
                "rank_items": rank_items,
            },
        }

        self.run_manager.write_json(run_id, "perf.json", out)
        self.run_manager.append_event(
            run_id=run_id,
            op="diagnostics",
            status="ok",
            input_payload={},
            output_file="perf.json",
            summary=f"tracked_stage_count={len(stage_ms)}",
        )
        return out

    def run_report(self, run_id: str) -> dict:
        t0 = time.perf_counter()
        self.run_score(run_id)
        self.run_diagnostics(run_id)
        out = self.run_manager.compile_report(run_id=run_id)
        self.run_manager.append_event(
            run_id=run_id,
            op="report",
            status="ok",
            input_payload={},
            output_file="report.json",
            summary="compiled report.md + report.json",
            meta={"perf": {"duration_ms": _ms_since(t0)}},
        )
        return out

    def run_mine(
        self,
        query: str,
        search_top_k: int = 16,
        min_seed_count: int = 16,
        crossref_rows: int = 40,
        grow_limit_per_node: int = 20,
        round2_top_k: int = 12,
        pool_bias_strength: float = 0.8,
        seed_init_boost: float = 3.0,
        seed_pool_factor_floor: float = 0.06,
        seed_rescue_max: int = 8,
        anchor_score_boost: float = 4.0,
        backfill_limit_round1: int = 4000,
        backfill_limit_round2: int = 6000,
    ) -> dict:
        t0 = time.perf_counter()
        meta = self.run_start(query)
        run_id = meta["run_id"]
        run_dir = self.run_manager.run_dir(run_id)
        stage_log_path = run_dir / "pipeline_mine.log.jsonl"

        stage_rows: list[dict[str, Any]] = []

        def _log(stage: str, payload: dict[str, Any]) -> None:
            row = {"ts": _now_iso(), "stage": stage, **payload}
            stage_rows.append(row)
            with stage_log_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

        # Round 1
        t = time.perf_counter()
        search1 = self.run_search(
            run_id,
            prompt=query,
            top_k=search_top_k,
            min_seed_count=min_seed_count,
            crossref_rows=crossref_rows,
        )
        self.run_manager.write_json(run_id, "search_round1.json", search1)
        _log("round1_search", {"duration_ms": _ms_since(t), "new_paper_count": int(search1.get("new_paper_count") or 0)})

        t = time.perf_counter()
        grow1 = self.run_grow(run_id, levels=1, limit_per_node=grow_limit_per_node, seed_strategy="search_pool")
        self.run_manager.write_json(run_id, "grow_round1.json", grow1)
        _log("round1_grow", {"duration_ms": _ms_since(t), "total_discovered_unique": int(grow1.get("total_discovered_unique") or 0)})

        t = time.perf_counter()
        bf1 = self.graph_backfill_counts(run_id=run_id, limit=backfill_limit_round1, max_workers=4)
        self.run_manager.write_json(run_id, "counts_backfill_round1.json", bf1)
        _log("round1_backfill", {"duration_ms": _ms_since(t), **bf1})

        t = time.perf_counter()
        pool_size1 = len(self.run_manager.list_pool_papers(run_id))
        rank1 = self.run_rank(
            run_id,
            limit=max(1, pool_size1),
            mode="pool",
            pool_bias_strength=pool_bias_strength,
            seed_init_boost=seed_init_boost,
            seed_pool_factor_floor=seed_pool_factor_floor,
            seed_rescue_max=seed_rescue_max,
            anchor_score_boost=anchor_score_boost,
        )
        self.run_manager.write_json(run_id, "rank_round1_full.json", rank1)
        with (run_dir / "rank_round1_full.log").open("w", encoding="utf-8") as f:
            for it in rank1.get("items") or []:
                f.write(
                    f"{int(it.get('rank') or 0):04d}\t{float(it.get('score') or 0.0):.10f}\t{it.get('rank_over_total') or ''}\t"
                    f"{it.get('doi') or ''}\t{it.get('title') or ''}\n"
                )
        _log("round1_rank", {"duration_ms": _ms_since(t), "pool_size": pool_size1, "rank_items": len(rank1.get("items") or [])})

        # Round 2 seed union: initial search seeds + top-k ranked papers from round1
        initial_seed_dois = [
            _normalize_doi(p.get("doi"))
            for p in (search1.get("new_papers") or [])
            if _normalize_doi(p.get("doi"))
        ]
        ranked_top_dois: list[str] = []
        for it in (rank1.get("items") or [])[: max(1, min(int(round2_top_k), 200))]:
            d = _normalize_doi(it.get("doi"))
            if d:
                ranked_top_dois.append(d)
        seed_union = list(dict.fromkeys(initial_seed_dois + ranked_top_dois))
        seed_union_obj = {
            "round2_top_k": int(round2_top_k),
            "initial_seed_count": len(initial_seed_dois),
            "rank_top_k_count": len(ranked_top_dois),
            "union_count": len(seed_union),
            "seed_dois": seed_union,
        }
        self.run_manager.write_json(run_id, "round2_seed_union.json", seed_union_obj)
        _log("round2_seed_prep", seed_union_obj)

        # Round 2 search: query each top-k round1 paper title individually, then merge.
        t = time.perf_counter()
        title_queries: list[str] = []
        seen_titles: set[str] = set()
        for it in (rank1.get("items") or [])[: max(1, min(int(round2_top_k), 200))]:
            q = (it.get("title") or "").strip()
            if not q:
                continue
            k = _normalize_text(q)
            if not k or k in seen_titles:
                continue
            seen_titles.add(k)
            title_queries.append(q)

        per_title: list[dict[str, Any]] = []
        merged_round2_papers: list[dict[str, Any]] = []
        for q in title_queries:
            t_one = time.perf_counter()
            try:
                one = self.op_search(
                    prompt=q,
                    top_k=search_top_k,
                    min_seed_count=min_seed_count,
                    crossref_rows=crossref_rows,
                )
                per_title.append(
                    {
                        "title_query": q,
                        "ok": True,
                        "duration_ms": _ms_since(t_one),
                        "new_paper_count": int(one.get("new_paper_count") or 0),
                        "query_id": one.get("query_id"),
                        "items": one.get("new_papers") or [],
                    }
                )
                merged_round2_papers = self._merge_unique_seeds(merged_round2_papers, one.get("new_papers") or [], limit=5000)
            except Exception as e:
                per_title.append(
                    {
                        "title_query": q,
                        "ok": False,
                        "duration_ms": _ms_since(t_one),
                        "error": str(e),
                        "new_paper_count": 0,
                        "items": [],
                    }
                )

        search2 = {
            "operation": "search_round2",
            "strategy": "topk_titles_individual_search",
            "title_query_count": len(title_queries),
            "title_queries": title_queries,
            "per_title": per_title,
            "new_papers": merged_round2_papers,
            "new_paper_count": len(merged_round2_papers),
        }
        self.run_manager.write_json(run_id, "search_round2.json", search2)
        pool_stats_search2 = self.run_manager.upsert_pool(run_id, papers=search2.get("new_papers") or [], source_op="search_round2")
        self.run_manager.append_event(
            run_id=run_id,
            op="search_round2",
            status="ok",
            input_payload={
                "strategy": "topk_titles_individual_search",
                "title_query_count": len(title_queries),
                "top_k": search_top_k,
                "min_seed_count": min_seed_count,
                "crossref_rows": crossref_rows,
            },
            output_file="search_round2.json",
            summary=f"new_paper_count={search2.get('new_paper_count', 0)} pool_size={pool_stats_search2.get('pool_size', 0)}",
        )
        _log(
            "round2_search",
            {
                "duration_ms": _ms_since(t),
                "strategy": "topk_titles_individual_search",
                "title_query_count": len(title_queries),
                "new_paper_count": int(search2.get("new_paper_count") or 0),
                "pool": pool_stats_search2,
            },
        )

        # Round 2 grow using seed union
        t = time.perf_counter()
        grow2 = self.op_grow(seeds=seed_union, levels=1, limit_per_node=grow_limit_per_node, use_mock=False)
        self.run_manager.write_json(run_id, "grow_round2.json", grow2)
        grow2_papers = []
        for lv in grow2.get("results", []) or []:
            for p in lv.get("papers", []) or []:
                grow2_papers.append(
                    {
                        "paper_id": p.get("paper_id"),
                        "doi": p.get("doi"),
                        "title": p.get("title"),
                        "journal": p.get("venue"),
                        "publication_date": (f"{p.get('year')}-01-01" if p.get("year") else None),
                        "source": p.get("source") or "grow_round2",
                        "abstract": "",
                    }
                )
        pool_stats_grow2 = self.run_manager.upsert_pool(run_id, papers=grow2_papers, source_op="grow_round2")
        self.run_manager.append_event(
            run_id=run_id,
            op="grow_round2",
            status="ok",
            input_payload={"seed_count": len(seed_union), "levels": 1, "limit_per_node": grow_limit_per_node},
            output_file="grow_round2.json",
            summary=f"total_discovered_unique={grow2.get('total_discovered_unique', 0)} pool_size={pool_stats_grow2.get('pool_size', 0)}",
        )
        _log("round2_grow", {"duration_ms": _ms_since(t), "total_discovered_unique": int(grow2.get("total_discovered_unique") or 0), "pool": pool_stats_grow2})

        t = time.perf_counter()
        bf2 = self.graph_backfill_counts(run_id=run_id, limit=backfill_limit_round2, max_workers=4)
        self.run_manager.write_json(run_id, "counts_backfill_round2.json", bf2)
        _log("round2_backfill", {"duration_ms": _ms_since(t), **bf2})

        t = time.perf_counter()
        pool_size2 = len(self.run_manager.list_pool_papers(run_id))
        rank2 = self.run_rank(
            run_id,
            limit=max(1, pool_size2),
            mode="pool",
            pool_bias_strength=pool_bias_strength,
            seed_init_boost=seed_init_boost,
            seed_pool_factor_floor=seed_pool_factor_floor,
            seed_rescue_max=seed_rescue_max,
            anchor_score_boost=anchor_score_boost,
        )
        self.run_manager.write_json(run_id, "rank_round2_full.json", rank2)
        with (run_dir / "rank_round2_full.log").open("w", encoding="utf-8") as f:
            for it in rank2.get("items") or []:
                f.write(
                    f"{int(it.get('rank') or 0):04d}\t{float(it.get('score') or 0.0):.10f}\t{it.get('rank_over_total') or ''}\t"
                    f"{it.get('doi') or ''}\t{it.get('title') or ''}\n"
                )
        _log("round2_rank", {"duration_ms": _ms_since(t), "pool_size": pool_size2, "rank_items": len(rank2.get("items") or [])})

        report = self.run_report(run_id)

        summary = {
            "operation": "mine",
            "run_id": run_id,
            "query": query,
            "top_k_round2": int(round2_top_k),
            "pool_size_round1": pool_size1,
            "pool_size_round2": pool_size2,
            "round1_top": (rank1.get("items") or [])[:10],
            "round2_top": (rank2.get("items") or [])[:10],
            "artifacts": {
                "stage_log": str(stage_log_path),
                "search_round1": str(run_dir / "search_round1.json"),
                "grow_round1": str(run_dir / "grow_round1.json"),
                "counts_backfill_round1": str(run_dir / "counts_backfill_round1.json"),
                "rank_round1_full": str(run_dir / "rank_round1_full.json"),
                "rank_round1_log": str(run_dir / "rank_round1_full.log"),
                "round2_seed_union": str(run_dir / "round2_seed_union.json"),
                "search_round2": str(run_dir / "search_round2.json"),
                "grow_round2": str(run_dir / "grow_round2.json"),
                "counts_backfill_round2": str(run_dir / "counts_backfill_round2.json"),
                "rank_round2_full": str(run_dir / "rank_round2_full.json"),
                "rank_round2_log": str(run_dir / "rank_round2_full.log"),
                "report_md": report.get("report_md"),
                "report_json": report.get("report_json"),
            },
            "duration_ms": _ms_since(t0),
        }
        self.run_manager.write_json(run_id, "mine_summary.json", summary)
        self.run_manager.append_event(
            run_id=run_id,
            op="mine",
            status="ok",
            input_payload={
                "query": query,
                "search_top_k": search_top_k,
                "min_seed_count": min_seed_count,
                "crossref_rows": crossref_rows,
                "grow_limit_per_node": grow_limit_per_node,
                "round2_top_k": round2_top_k,
                "anchor_score_boost": anchor_score_boost,
            },
            output_file="mine_summary.json",
            summary=f"pool_round1={pool_size1} pool_round2={pool_size2}",
            meta={"perf": {"duration_ms": _ms_since(t0)}},
        )
        return summary

    def relevance_classify_query_id(
        self,
        topic: str,
        query_id: str,
        top_k: int = 20,
        sort: str = "RelevanceScore",
        provider: str | None = DEFAULT_LLM_PROVIDER,
        model: str | None = DEFAULT_LLM_MODEL,
        thinking: str | None = DEFAULT_LLM_THINKING,
        max_workers: int = 2,
    ) -> dict:
        topic = (topic or "").strip()
        if len(topic) < 3:
            raise ValueError("topic must be at least 3 chars")

        papers = self.bohrium_question_papers(query_id=query_id, sort=sort)
        items = list(papers.get("items") or [])

        def _score(it: dict) -> float:
            rs = it.get("relevance_score")
            ss = it.get("sort_score")
            try:
                if rs is not None:
                    return float(rs)
                if ss is not None:
                    return float(ss)
            except Exception:
                pass
            return 0.0

        items.sort(key=_score, reverse=True)
        batch = items[: max(1, min(int(top_k), 200))]

        def _classify_one(it: dict) -> dict:
            abstract = (it.get("abstract") or "").strip()
            if not abstract:
                return {
                    "title": it.get("title") or "",
                    "doi": _normalize_doi(it.get("doi")),
                    "paper_id": f"doi:{_normalize_doi(it.get('doi'))}" if _normalize_doi(it.get("doi")) else None,
                    "label": "non_classifiable",
                    "reason": "abstract_unavailable",
                    "llm_ok": False,
                    "journal": it.get("journal") or "",
                    "publication_date": it.get("publication_date"),
                    "relevance_score": it.get("relevance_score"),
                    "sort_score": it.get("sort_score"),
                }

            prompt = (
                "You are classifying paper relevance to a research topic. "
                "Return strict JSON only with keys: label, reason. "
                "label must be one of: highly_relevant, closely_related, ignorable. "
                "Be strict and conservative.\n"
                f"Topic:\n{topic}\n\n"
                f"Paper title:\n{it.get('title') or ''}\n\n"
                f"Paper abstract:\n{abstract}\n"
            )
            llm = self.llm_prompt(prompt=prompt, provider=provider, model=model, thinking=thinking)
            parsed = _parse_json_object(llm.get("response")) if llm.get("ok") else None
            label = (parsed or {}).get("label") if parsed else None
            label = str(label).strip().lower().replace(" ", "_") if label else ""
            if label not in ("highly_relevant", "closely_related", "ignorable"):
                label = "non_classifiable"
            reason = (parsed or {}).get("reason") if parsed else None

            doi = _normalize_doi(it.get("doi"))
            return {
                "title": it.get("title") or "",
                "doi": doi,
                "paper_id": f"doi:{doi}" if doi else None,
                "label": label,
                "reason": (reason or llm.get("stderr") or "").strip(),
                "llm_ok": bool(llm.get("ok")),
                "journal": it.get("journal") or "",
                "publication_date": it.get("publication_date"),
                "relevance_score": it.get("relevance_score"),
                "sort_score": it.get("sort_score"),
            }

        results = []
        max_workers = max(1, min(int(max_workers), 8))
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = [ex.submit(_classify_one, it) for it in batch]
            for fut in as_completed(futs):
                results.append(fut.result())

        order = {(it.get("title") or ""): idx for idx, it in enumerate(batch)}
        results.sort(key=lambda x: order.get(x.get("title") or "", 10**9))

        counts = {
            "highly_relevant": sum(1 for x in results if x.get("label") == "highly_relevant"),
            "closely_related": sum(1 for x in results if x.get("label") == "closely_related"),
            "ignorable": sum(1 for x in results if x.get("label") == "ignorable"),
            "non_classifiable": sum(1 for x in results if x.get("label") == "non_classifiable"),
        }

        return {
            "topic": topic,
            "query_id": str(query_id),
            "sort": sort,
            "provider": provider,
            "model": model,
            "thinking": thinking,
            "items_total": len(items),
            "items_classified": len(results),
            "counts": counts,
            "items": results,
            "bohrium_meta": {
                "code": papers.get("code"),
                "source_list": papers.get("source_list"),
                "log_id": papers.get("log_id"),
                "error": papers.get("error"),
            },
        }

    def graph_ingest_doi(self, doi: str, use_mock: bool = False) -> dict:
        out = ingest_doi(doi, use_mock=use_mock, fetch_assets=False)
        n = out.get("normalized", {})
        doi_norm = _normalize_doi(doi) or ""
        if not doi_norm:
            raise ValueError("doi is required")

        paper_id = f"doi:{doi_norm}"
        now = _now_iso()
        self.repo.upsert_api_paper(
            {
                "paper_id": paper_id,
                "doi": doi_norm,
                "openalex_id": None,
                "citation_count": None,
                "title": (n.get("title") or doi_norm),
                "year": _year_from_metadata(n),
                "venue": ((n.get("metadata") or {}).get("journal") or ""),
                "abstract": (n.get("abstract") or ""),
                "source": out.get("fetch", {}).get("source") or "ingest",
                "updated_at": now,
            }
        )

        base_refs = []
        for r in n.get("references", []) or []:
            raw_text = (r.get("text") or "").strip()
            ref_doi = _normalize_doi(r.get("doi")) or _extract_doi(raw_text)
            base_refs.append({"doi": ref_doi, "raw_text": raw_text})

        external = {}
        with ThreadPoolExecutor(max_workers=2) as ex:
            futures = {
                ex.submit(CrossrefClient().references_by_doi, doi_norm): "crossref",
                ex.submit(OpenAlexClient().references_by_doi, doi_norm): "openalex",
            }
            for fut in as_completed(futures):
                name = futures[fut]
                try:
                    external[name] = fut.result()
                except Exception as e:
                    external[name] = {"source": name, "references": [], "reference_count": 0, "citation_count": 0, "error": str(e)}

        extra_refs = []
        for src in ("crossref", "openalex"):
            extra_refs.extend(external.get(src, {}).get("references", []) or [])

        merged_refs = _merge_reference_rows(base_refs, extra_refs)

        oa_ref_count = int(external.get("openalex", {}).get("reference_count") or 0)
        oa_cite_count = int(external.get("openalex", {}).get("citation_count") or 0)
        cr_ref_count = int(external.get("crossref", {}).get("reference_count") or 0)
        cr_cite_count = int(external.get("crossref", {}).get("citation_count") or 0)

        # Fast + robust policy: OpenAlex primary, Crossref fallback.
        reference_count_global = _pick_count_primary_fallback(oa_ref_count, cr_ref_count)
        citation_count_global = _pick_count_primary_fallback(oa_cite_count, cr_cite_count)
        self.repo.upsert_api_paper(
            {
                "paper_id": paper_id,
                "doi": doi_norm,
                "openalex_id": None,
                "citation_count": citation_count_global,
                "reference_count": reference_count_global,
                "title": (n.get("title") or doi_norm),
                "year": _year_from_metadata(n),
                "venue": ((n.get("metadata") or {}).get("journal") or ""),
                "abstract": (n.get("abstract") or ""),
                "source": out.get("fetch", {}).get("source") or "ingest",
                "updated_at": now,
            }
        )

        refs = [
            {
                "src_paper_id": paper_id,
                "ref_order": idx,
                "doi": r.get("doi"),
                "ref_openalex_id": None,
                "raw_text": r.get("raw_text") or "",
            }
            for idx, r in enumerate(merged_refs, start=1)
        ]

        self.repo.replace_api_references(src_paper_id=paper_id, refs=refs)
        edge_count = self.repo.resolve_edges_for_src_paper(src_paper_id=paper_id, now_iso=now)

        return {
            "paper_id": paper_id,
            "doi": doi_norm,
            "reference_count": len(refs),
            "edge_count": edge_count,
            "fetch": out.get("fetch", {}),
            "quality": out.get("quality", {}),
            "enrichment": {
                "base_ref_count": len(base_refs),
                "crossref_ref_count": int(external.get("crossref", {}).get("reference_count") or 0),
                "openalex_ref_count": int(external.get("openalex", {}).get("reference_count") or 0),
                "crossref_error": external.get("crossref", {}).get("error"),
                "openalex_error": external.get("openalex", {}).get("error"),
            },
        }

    def graph_stats(self) -> dict:
        return self.repo.get_graph_stats()

    def graph_maintenance_full_resolve(self) -> dict:
        """Offline maintenance placeholder: full global edge consistency rebuild."""
        t0 = time.perf_counter()
        now = _now_iso()
        edge_count = self.repo.resolve_edges_doi_match(now_iso=now)
        return {
            "operation": "graph_maintenance_full_resolve",
            "edge_count": edge_count,
            "duration_ms": _ms_since(t0),
            "graph_stats": self.repo.get_graph_stats(),
        }

    def _resolve_arxiv_published_doi(self, doi: str | None) -> str | None:
        d = _normalize_doi(doi)
        if not d or not d.startswith("10.48550/arxiv."):
            return None
        arxiv_id = d[len("10.48550/arxiv.") :].strip()
        if not arxiv_id:
            return None
        try:
            url = f"http://export.arxiv.org/api/query?id_list={arxiv_id}"
            req = urllib.request.Request(url, headers={"User-Agent": "curl/8.5.0"}, method="GET")
            raw = urllib.request.urlopen(req, timeout=20).read()
            root = ET.fromstring(raw)
            ns = {"a": "http://www.w3.org/2005/Atom", "arxiv": "http://arxiv.org/schemas/atom"}
            entry = root.find("a:entry", ns)
            if entry is None:
                return None
            pd = (entry.findtext("arxiv:doi", default="", namespaces=ns) or "").strip()
            pd = _normalize_doi(pd)
            if pd and pd != d:
                return pd
        except Exception:
            return None
        return None

    def graph_backfill_counts(self, run_id: str | None = None, limit: int = 500, max_workers: int = 4) -> dict:
        """In-place metadata refresh for older rows missing citation/reference counts.

        Fast+robust policy:
          - OpenAlex counts primary
          - Crossref counts fallback
        """
        t0 = time.perf_counter()
        max_workers = max(1, min(int(max_workers), 8))
        limit = max(1, min(int(limit), 5000))

        if run_id:
            pool_rows = self.run_manager.list_pool_papers(run_id)
            dois = [
                _normalize_doi(x.get("doi"))
                for x in pool_rows
                if _normalize_doi(x.get("doi"))
            ]
            dois = list(dict.fromkeys(dois))[:limit]
            candidates = [{"doi": d} for d in dois]
        else:
            candidates = self.repo.list_api_papers_missing_counts(limit=limit)
            dois = [
                _normalize_doi(x.get("doi"))
                for x in candidates
                if _normalize_doi(x.get("doi"))
            ]

        oa = OpenAlexClient()
        cr = CrossrefClient()
        now = _now_iso()

        def _fetch_counts(doi: str) -> dict[str, Any]:
            candidates = [doi]
            alias = self._resolve_arxiv_published_doi(doi)
            if alias and alias not in candidates:
                candidates.append(alias)

            best: dict[str, Any] | None = None
            best_score = -1

            for qdoi in candidates:
                ow = oa.work_by_doi(qdoi)
                oc = int(_safe_float(ow.get("citation_count"), 0.0))
                orf = int(_safe_float(ow.get("reference_count"), 0.0))

                cw = cr.references_by_doi(qdoi)
                cc = int(_safe_float(cw.get("citation_count"), 0.0))
                crf = int(_safe_float(cw.get("reference_count"), 0.0))

                cite = _pick_count_primary_fallback(oc, cc)
                ref = _pick_count_primary_fallback(orf, crf)
                src = "openalex" if (oc > 0 or orf > 0 or (ow.get("title") or "").strip()) else ("crossref" if (cc > 0 or crf > 0 or (cw.get("title") or "").strip()) else "none")

                score = int(cite + ref)
                if (ow.get("title") or "").strip():
                    score += 3
                if src == "openalex":
                    score += 1

                row = {
                    "doi": doi,
                    "query_doi": qdoi,
                    "alias_used": bool(alias and qdoi == alias),
                    "cite": cite,
                    "ref": ref,
                    "src": src,
                    "oa": ow,
                    "cr": cw,
                }
                if score > best_score:
                    best_score = score
                    best = row

            return best or {"doi": doi, "query_doi": doi, "alias_used": False, "cite": 0, "ref": 0, "src": "none", "oa": {}, "cr": {}}

        updated = 0
        missing = 0
        source_counts = {"openalex": 0, "crossref": 0, "none": 0}
        metadata_overwritten = 0
        alias_used_count = 0

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = {ex.submit(_fetch_counts, d): d for d in dois}
            for fut in as_completed(futs):
                d = futs[fut]
                try:
                    got = fut.result() or {}
                    doi = _normalize_doi(got.get("doi")) or d
                    cite = int(_safe_float(got.get("cite"), 0.0))
                    ref = int(_safe_float(got.get("ref"), 0.0))
                    src = str(got.get("src") or "none")
                    ow = got.get("oa") or {}
                    cw = got.get("cr") or {}
                    if bool(got.get("alias_used")):
                        alias_used_count += 1
                except Exception:
                    missing += 1
                    continue

                source_counts[src] = source_counts.get(src, 0) + 1

                # Overwrite/fill core metadata (title/year/venue/source) from OA primary, CR fallback.
                existing = self.repo.get_api_paper_by_doi(doi)
                ex_title = (existing["title"] if existing and existing["title"] is not None else "") if existing else ""
                ex_year = int(existing["year"]) if existing and existing["year"] is not None else None
                ex_venue = (existing["venue"] if existing and existing["venue"] is not None else "") if existing else ""
                ex_abstract = (existing["abstract"] if existing and existing["abstract"] is not None else "") if existing else ""
                ex_source = (existing["source"] if existing and existing["source"] is not None else "") if existing else ""
                ex_cite = int(existing["citation_count"]) if existing and existing["citation_count"] is not None else 0
                ex_ref = int(existing["reference_count"]) if existing and existing["reference_count"] is not None else 0

                oa_title = (ow.get("title") or "").strip()
                cr_title = (cw.get("title") or "").strip()
                title = oa_title or cr_title or ex_title or doi
                if _is_likely_doi(title):
                    title = ex_title if ex_title and not _is_likely_doi(ex_title) else title

                year = _year_from_any(ow.get("year")) or _year_from_any(cw.get("year")) or ex_year
                venue = (ow.get("venue") or "").strip() or (cw.get("venue") or "").strip() or ex_venue
                abstract = (ow.get("abstract") or "").strip() or ex_abstract
                row_source = "openalex" if oa_title else ("crossref" if cr_title else (ex_source or src or "openalex"))

                out_cite = cite if cite > 0 else ex_cite
                out_ref = ref if ref > 0 else ex_ref

                self.repo.upsert_api_paper(
                    {
                        "paper_id": (existing["paper_id"] if existing and existing["paper_id"] else f"doi:{doi}"),
                        "doi": doi,
                        "openalex_id": None,
                        "citation_count": out_cite,
                        "reference_count": out_ref,
                        "title": title or doi,
                        "year": year,
                        "venue": venue,
                        "abstract": abstract,
                        "source": row_source,
                        "updated_at": now,
                    }
                )
                metadata_overwritten += 1

                if out_cite <= 0 and out_ref <= 0:
                    missing += 1
                    continue
                updated += 1

        out = {
            "operation": "graph_backfill_counts",
            "run_id": run_id,
            "attempted": len(dois),
            "updated": updated,
            "missing": missing,
            "metadata_overwritten": metadata_overwritten,
            "alias_used_count": alias_used_count,
            "source_counts": source_counts,
            "duration_ms": _ms_since(t0),
        }
        if run_id:
            self.run_manager.write_json(run_id, "counts_backfill.json", out)
            self.run_manager.append_event(
                run_id=run_id,
                op="counts_backfill",
                status="ok",
                input_payload={"limit": limit, "max_workers": max_workers},
                output_file="counts_backfill.json",
                summary=f"updated={updated}/{len(dois)}",
            )
        return out

    def graph_ingest_openalex_journals(self, journals: list[str], per_journal: int = 10) -> dict:
        journals = [j.strip() for j in journals if j and j.strip()]
        if not journals:
            raise ValueError("journals is required")
        per_journal = max(1, min(int(per_journal), 100))

        oa = OpenAlexClient()
        now = _now_iso()
        summary = []
        total_papers = 0
        total_refs = 0

        for journal in journals:
            sid = oa.resolve_source_id(journal)
            if not sid:
                summary.append({"journal": journal, "source_id": None, "papers_ingested": 0, "references_ingested": 0, "error": "source_not_found"})
                continue

            works = oa.works_by_source(sid, max_results=per_journal)
            p_ingested = 0
            r_ingested = 0
            src_pids: list[str] = []

            for w in works:
                doi = _normalize_doi(w.get("doi"))
                if not doi:
                    continue
                pid = f"doi:{doi}"
                title = (w.get("display_name") or doi).strip()
                year = w.get("publication_year")
                venue = ((w.get("primary_location") or {}).get("source") or {}).get("display_name") or journal
                abstract = ""

                self.repo.upsert_api_paper(
                    {
                        "paper_id": pid,
                        "doi": doi,
                        "openalex_id": OpenAlexClient._openalex_id(w.get("id")),
                        "citation_count": int(w.get("cited_by_count") or 0),
                        "title": title,
                        "year": year,
                        "venue": venue,
                        "abstract": abstract,
                        "source": "openalex",
                        "updated_at": now,
                    }
                )
                p_ingested += 1

                ref_payload = oa.references_by_doi(doi)
                refs = []
                for idx, rr in enumerate(ref_payload.get("references", []) or [], start=1):
                    refs.append(
                        {
                            "src_paper_id": pid,
                            "ref_order": idx,
                            "doi": _normalize_doi(rr.get("doi")),
                            "ref_openalex_id": None,
                            "raw_text": (rr.get("raw_text") or "").strip(),
                        }
                    )
                self.repo.replace_api_references(src_paper_id=pid, refs=refs)
                self.repo.upsert_api_paper(
                    {
                        "paper_id": pid,
                        "doi": doi,
                        "openalex_id": OpenAlexClient._openalex_id(w.get("id")),
                        "citation_count": int(w.get("cited_by_count") or 0),
                        "reference_count": int(ref_payload.get("reference_count") or len(refs)),
                        "title": title,
                        "year": year,
                        "venue": venue,
                        "abstract": abstract,
                        "source": "openalex",
                        "updated_at": now,
                    }
                )
                src_pids.append(pid)
                r_ingested += len(refs)

            if src_pids:
                self.repo.resolve_edges_for_src_papers(src_paper_ids=src_pids, now_iso=now)

            total_papers += p_ingested
            total_refs += r_ingested
            summary.append({"journal": journal, "source_id": sid, "papers_ingested": p_ingested, "references_ingested": r_ingested, "error": None})

        graph_stats = self.repo.get_graph_stats()
        return {
            "journals": summary,
            "total_papers_ingested": total_papers,
            "total_references_ingested": total_refs,
            "edge_count": graph_stats.get("edge_count"),
            "graph_stats": graph_stats,
        }

    def graph_backfill_openalex_journal(self, journal: str, max_results: int | None = None, per_page: int = 200) -> dict:
        journal = (journal or "").strip()
        if not journal:
            raise ValueError("journal is required")

        oa = OpenAlexClient()
        sid = oa.resolve_source_id(journal)
        if not sid:
            raise ValueError("source_not_found")

        now = _now_iso()
        ingested = 0
        skipped_no_doi = 0
        refs_ingested = 0
        processed = 0
        src_pids: list[str] = []

        error = None
        try:
            for works, _meta in oa.iter_works_by_source(source_id=sid, per_page=per_page):
                for w in works:
                    processed += 1
                    if max_results is not None and ingested >= max_results:
                        break

                    doi = _normalize_doi(w.get("doi"))
                    if not doi:
                        skipped_no_doi += 1
                        continue

                    openalex_id = OpenAlexClient._openalex_id(w.get("id"))
                    pid = f"doi:{doi}"
                    self.repo.upsert_api_paper(
                        {
                            "paper_id": pid,
                            "doi": doi,
                            "openalex_id": openalex_id,
                            "citation_count": int(w.get("cited_by_count") or 0),
                            "reference_count": len(w.get("referenced_works") or []),
                            "title": (w.get("display_name") or doi).strip(),
                            "year": w.get("publication_year"),
                            "venue": ((w.get("primary_location") or {}).get("source") or {}).get("display_name") or journal,
                            "abstract": "",
                            "source": "openalex",
                            "updated_at": now,
                        }
                    )

                    refs = []
                    for idx, rid in enumerate((w.get("referenced_works") or []), start=1):
                        refs.append(
                            {
                                "src_paper_id": pid,
                                "ref_order": idx,
                                "doi": None,
                                "ref_openalex_id": OpenAlexClient._openalex_id(rid),
                                "raw_text": "",
                            }
                        )
                    self.repo.replace_api_references(src_paper_id=pid, refs=refs)
                    src_pids.append(pid)
                    refs_ingested += len(refs)
                    ingested += 1

                if max_results is not None and ingested >= max_results:
                    break
        except ProviderError as e:
            error = str(e)

        if src_pids:
            self.repo.resolve_edges_for_src_papers(src_paper_ids=src_pids, now_iso=now)
        graph_stats = self.repo.get_graph_stats()
        return {
            "journal": journal,
            "source_id": sid,
            "papers_ingested": ingested,
            "papers_processed": processed,
            "papers_skipped_no_doi": skipped_no_doi,
            "references_ingested": refs_ingested,
            "edge_count": graph_stats.get("edge_count"),
            "error": error,
            "graph_stats": graph_stats,
        }

    def graph_expand(
        self,
        seeds: list[str],
        rounds: int = 1,
        max_new_per_round: int = 100,
        use_mock: bool = False,
        max_workers: int = 2,
    ) -> dict:
        if not seeds:
            raise ValueError("seeds is required")
        rounds = max(1, min(int(rounds), 10))
        max_new_per_round = max(1, min(int(max_new_per_round), 1000))
        max_workers = max(1, min(int(max_workers), 8))

        resolved_seeds = [self._resolve_seed(s) for s in seeds]
        frontier = list(resolved_seeds)
        seen = set(frontier)
        round_summaries = []

        for r in range(1, rounds + 1):
            candidate_dois = self.repo.get_missing_reference_dois(frontier, limit=max_new_per_round * 5)
            candidate_dois = list(dict.fromkeys(candidate_dois))[:max_new_per_round]
            if not candidate_dois:
                round_summaries.append({"round": r, "candidates": 0, "ingested": 0, "errors": 0})
                break

            ingested_ids = []
            errors = 0
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futures = {ex.submit(self.graph_ingest_doi, doi, use_mock): doi for doi in candidate_dois}
                for fut in as_completed(futures):
                    doi = futures[fut]
                    try:
                        out = fut.result()
                        pid = out.get("paper_id")
                        if pid and pid not in seen:
                            seen.add(pid)
                            ingested_ids.append(pid)
                    except Exception:
                        _ = doi
                        errors += 1

            frontier = ingested_ids
            round_summaries.append(
                {
                    "round": r,
                    "candidates": len(candidate_dois),
                    "ingested": len(ingested_ids),
                    "errors": errors,
                }
            )
            if not frontier:
                break

        return {
            "seeds": resolved_seeds,
            "rounds_requested": rounds,
            "max_new_per_round": max_new_per_round,
            "rounds": round_summaries,
            "graph_stats": self.repo.get_graph_stats(),
        }

    def graph_rank(
        self,
        seeds: list[str],
        limit: int = 20,
        alpha: float = 0.85,
        max_iter: int = 100,
        tol: float = 1e-7,
        include_seeds: bool = False,
        venue_prior: bool = True,
        same_venue_boost: float = 0.20,
        related_venue_boost: float = 0.08,
    ) -> dict:
        if not seeds:
            raise ValueError("seeds is required")
        if not (0.0 < alpha < 1.0):
            raise ValueError("alpha must be between 0 and 1")

        seed_ids = [self._resolve_seed(s) for s in seeds]
        seed_ids = list(dict.fromkeys(seed_ids))

        nodes = self.repo.get_all_paper_ids()
        if not nodes:
            return {"seeds": seed_ids, "items": [], "iterations": 0, "converged": True}

        node_set = set(nodes)
        for s in seed_ids:
            if s not in node_set:
                raise KeyError(f"seed not found in graph: {s}")

        edges = self.repo.get_all_edges()
        out_adj: dict[str, list[str]] = {n: [] for n in nodes}
        in_adj: dict[str, list[str]] = {n: [] for n in nodes}
        undirected: dict[str, list[str]] = {n: [] for n in nodes}
        for src, dst in edges:
            if src not in node_set or dst not in node_set:
                continue
            out_adj[src].append(dst)
            in_adj[dst].append(src)
            undirected[src].append(dst)
            undirected[dst].append(src)

        n = float(len(nodes))
        pers = {k: 0.0 for k in nodes}
        for s in seed_ids:
            pers[s] = 1.0 / float(len(seed_ids))

        pr = {k: 1.0 / n for k in nodes}
        converged = False
        iters = 0
        for i in range(max_iter):
            iters = i + 1
            new_pr = {k: (1.0 - alpha) * pers[k] for k in nodes}
            dangling_mass = sum(pr[k] for k in nodes if not out_adj[k])
            for k in nodes:
                new_pr[k] += alpha * dangling_mass * pers[k]

            for src in nodes:
                outs = out_adj[src]
                if not outs:
                    continue
                share = alpha * pr[src] / float(len(outs))
                for dst in outs:
                    new_pr[dst] += share

            delta = sum(abs(new_pr[k] - pr[k]) for k in nodes)
            pr = new_pr
            if delta < tol:
                converged = True
                break

        from collections import deque

        dist = {s: 0 for s in seed_ids}
        q = deque(seed_ids)
        while q:
            u = q.popleft()
            for v in undirected.get(u, []):
                if v in dist:
                    continue
                dist[v] = dist[u] + 1
                q.append(v)

        def _venue_norm(v: str | None) -> str:
            return (v or "").strip().lower()

        def _venue_groups(v: str | None) -> set[str]:
            x = _venue_norm(v)
            groups = set()
            if "fuel" in x:
                groups.add("fuel")
            if "combust" in x:
                groups.add("combustion")
            if "energy" in x:
                groups.add("energy")
            if "proceedings" in x and "combust" in x:
                groups.add("combustion")
            return groups

        candidates = []
        for pid, score in pr.items():
            if not include_seeds and pid in seed_ids:
                continue
            candidates.append((pid, score))

        candidate_ids = [pid for pid, _ in candidates]
        meta_rows = self.repo.get_api_papers_by_ids(candidate_ids + seed_ids)
        meta_map = {r["paper_id"]: r for r in meta_rows}
        seed_set = set(seed_ids)

        seed_venues = {_venue_norm(meta_map.get(s, {}).get("venue")) for s in seed_ids}
        seed_venues.discard("")
        seed_groups = set()
        for sv in seed_venues:
            seed_groups |= _venue_groups(sv)

        rescored = []
        for pid, score in candidates:
            m = meta_map.get(pid, {})
            venue = _venue_norm(m.get("venue"))
            boost = 0.0
            if venue_prior and venue:
                if venue in seed_venues:
                    boost = same_venue_boost
                elif _venue_groups(venue) & seed_groups:
                    boost = related_venue_boost
            rescored.append((pid, float(score), float(score) * (1.0 + boost), boost))

        rescored.sort(key=lambda x: x[2], reverse=True)
        top = rescored[: max(1, min(int(limit), 200))]

        items = []
        for pid, raw_score, score, boost in top:
            m = meta_map.get(pid, {"paper_id": pid, "doi": None, "title": pid, "year": None, "venue": None, "source": None, "citation_count": None})
            direct_from_seed = sum(1 for s in seed_set if pid in out_adj.get(s, []))
            direct_to_seed = sum(1 for s in seed_set if s in out_adj.get(pid, []))
            items.append(
                {
                    "paper_id": pid,
                    "doi": m.get("doi"),
                    "title": m.get("title"),
                    "year": m.get("year"),
                    "venue": m.get("venue"),
                    "source": m.get("source"),
                    "citation_count": m.get("citation_count"),
                    "score": round(float(score), 8),
                    "explain": {
                        "base_ppr_score": round(float(raw_score), 8),
                        "venue_boost": round(float(boost), 4),
                        "distance_from_seed": dist.get(pid),
                        "direct_from_seed_count": direct_from_seed,
                        "direct_to_seed_count": direct_to_seed,
                        "in_degree_local": len(in_adj.get(pid, [])),
                        "out_degree_local": len(out_adj.get(pid, [])),
                    },
                }
            )

        return {
            "seeds": seed_ids,
            "params": {
                "limit": max(1, min(int(limit), 200)),
                "alpha": alpha,
                "max_iter": max_iter,
                "tol": tol,
                "include_seeds": include_seeds,
                "venue_prior": venue_prior,
                "same_venue_boost": same_venue_boost,
                "related_venue_boost": related_venue_boost,
            },
            "iterations": iters,
            "converged": converged,
            "items": items,
        }

    def graph_neighbors(self, seed: str, direction: str = "both", limit: int = 50) -> dict:
        pid = self._resolve_seed(seed)
        if direction not in ("in", "out", "both"):
            raise ValueError("direction must be one of: in,out,both")
        return {
            "seed": pid,
            "direction": direction,
            "neighbors": self.repo.graph_neighbors(pid, direction=direction, limit=max(1, min(limit, 200))),
        }

    def graph_related(self, seed: str, mode: str = "coupling", limit: int = 20) -> dict:
        pid = self._resolve_seed(seed)
        if mode == "coupling":
            items = self.repo.graph_related_coupling(pid, limit=max(1, min(limit, 200)))
        elif mode == "cocite":
            items = self.repo.graph_related_cocite(pid, limit=max(1, min(limit, 200)))
        else:
            raise ValueError("mode must be one of: coupling,cocite")
        return {"seed": pid, "mode": mode, "items": items}

    def graph_prior(self, seed: str, direction: str = "both", limit: int = 50) -> dict:
        return self._graph_temporal(seed, relation="prior", direction=direction, limit=limit)

    def graph_derivative(self, seed: str, direction: str = "both", limit: int = 50) -> dict:
        return self._graph_temporal(seed, relation="derivative", direction=direction, limit=limit)

    def graph_related_set(self, seeds: list[str], mode: str = "coupling", limit: int = 20) -> dict:
        if not seeds:
            raise ValueError("seeds is required")
        seed_ids = [self._resolve_seed(s) for s in seeds]
        seed_ids = list(dict.fromkeys(seed_ids))

        if mode == "coupling":
            items = self.repo.graph_related_set_coupling(seed_ids, limit=max(1, min(limit, 200)))
        elif mode == "cocite":
            items = self.repo.graph_related_set_cocite(seed_ids, limit=max(1, min(limit, 200)))
        else:
            raise ValueError("mode must be one of: coupling,cocite")
        return {"seeds": seed_ids, "mode": mode, "items": items}

    def _graph_temporal(self, seed: str, relation: str, direction: str = "both", limit: int = 50) -> dict:
        pid = self._resolve_seed(seed)
        if direction not in ("in", "out", "both"):
            raise ValueError("direction must be one of: in,out,both")

        seed_row = self.repo.get_api_paper_by_id(pid)
        if not seed_row:
            raise KeyError("seed paper not found")
        seed_year = seed_row["year"]
        if seed_year is None:
            raise ValueError("seed paper year is unavailable")

        neighbors = self.repo.graph_neighbors(pid, direction=direction, limit=max(1, min(limit, 200)))

        def filt(rows: list[dict]) -> list[dict]:
            out = []
            for r in rows:
                y = r.get("year")
                if y is None:
                    continue
                if relation == "prior" and y < seed_year:
                    out.append(r)
                if relation == "derivative" and y > seed_year:
                    out.append(r)
            out.sort(key=lambda x: (x.get("year") or 0, x.get("paper_id") or ""), reverse=(relation == "derivative"))
            return out

        return {
            "seed": pid,
            "seed_year": seed_year,
            "relation": relation,
            "direction": direction,
            "neighbors": {
                "out": filt(neighbors.get("out", [])),
                "in": filt(neighbors.get("in", [])),
            },
        }

    def _resolve_seed(self, seed: str) -> str:
        seed = (seed or "").strip()
        if not seed:
            raise ValueError("seed is required")
        if seed.startswith("doi:"):
            return seed.lower()
        if seed.startswith("10."):
            row = self.repo.get_api_paper_by_doi(seed)
            if not row:
                raise KeyError("paper not found for doi")
            return row["paper_id"]
        if seed.upper().startswith("W"):
            row = self.repo.get_api_paper_by_openalex_id(seed)
            if not row:
                raise KeyError("paper not found for openalex id")
            return row["paper_id"]
        return seed

    def _generate_results(self, search_id: str, query: str, n: int) -> list[dict]:
        seed = int(hashlib.sha256(query.encode("utf-8")).hexdigest()[:8], 16)
        rnd = random.Random(seed)
        out = []
        for i in range(n):
            score = max(0.05, min(0.99, rnd.random() * 0.8 + 0.15))
            if score > 0.8:
                relevance = "highly_relevant"
            elif score > 0.45:
                relevance = "closely_related"
            else:
                relevance = "ignorable"
            out.append(
                {
                    "search_id": search_id,
                    "paper_id": f"mock:{seed % 10000:04d}.{i:04d}",
                    "title": f"{query[:60]} — candidate paper #{i + 1}",
                    "score": score,
                    "relevance": relevance,
                    "why": f"Synthetic ranking signal from query intent for '{query[:24]}...'",
                }
            )
        out.sort(key=lambda r: r["score"], reverse=True)
        return out
