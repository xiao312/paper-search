from __future__ import annotations

import hashlib
import json
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from difflib import SequenceMatcher
from typing import Optional
from uuid import uuid4

from papersearch.app.repository import Repo
from papersearch.adapters.feishu.notifier import FeishuNotifier
from papersearch.ingest.discovery_bohrium import BohriumSigmaSearchClient
from papersearch.ingest.discovery_crossref import CrossrefClient
from papersearch.ingest.discovery_openalex import OpenAlexClient
from papersearch.ingest.errors import ProviderError
from papersearch.ingest.pipeline import ingest_doi
from papersearch.integrations.pi_mono_client import PiMonoClient


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
    def __init__(self, repo: Optional[Repo] = None, notifier: Optional[FeishuNotifier] = None):
        self.repo = repo or Repo()
        self.notifier = notifier

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

    def llm_list_models(self, provider: str | None = None, search: str | None = None) -> dict:
        client = PiMonoClient()
        out = client.list_models(provider=provider, search=search)
        lines = [x for x in out.stdout.splitlines() if x.strip()]
        return {
            "ok": out.ok,
            "provider": provider,
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
        client = PiMonoClient()
        out = client.prompt(prompt=prompt, provider=provider, model=model, thinking=thinking)
        return {
            "ok": out.ok,
            "provider": provider,
            "model": model,
            "thinking": thinking,
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
            text = f"{it.get('title') or ''} {it.get('journal') or ''}".lower()
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

    def relevance_classify_query_id(
        self,
        topic: str,
        query_id: str,
        top_k: int = 20,
        sort: str = "RelevanceScore",
        provider: str | None = "openai-codex",
        model: str | None = "gpt-5.1-codex-mini",
        thinking: str | None = "none",
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
        edge_count = self.repo.resolve_edges_doi_match(now_iso=now)

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
                r_ingested += len(refs)

            total_papers += p_ingested
            total_refs += r_ingested
            summary.append({"journal": journal, "source_id": sid, "papers_ingested": p_ingested, "references_ingested": r_ingested, "error": None})

        edge_count = self.repo.resolve_edges_doi_match(now_iso=now)
        return {
            "journals": summary,
            "total_papers_ingested": total_papers,
            "total_references_ingested": total_refs,
            "edge_count": edge_count,
            "graph_stats": self.repo.get_graph_stats(),
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
                    refs_ingested += len(refs)
                    ingested += 1

                if max_results is not None and ingested >= max_results:
                    break
        except ProviderError as e:
            error = str(e)

        edge_count = self.repo.resolve_edges_doi_match(now_iso=now)
        return {
            "journal": journal,
            "source_id": sid,
            "papers_ingested": ingested,
            "papers_processed": processed,
            "papers_skipped_no_doi": skipped_no_doi,
            "references_ingested": refs_ingested,
            "edge_count": edge_count,
            "error": error,
            "graph_stats": self.repo.get_graph_stats(),
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
