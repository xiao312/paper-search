from __future__ import annotations

import hashlib
import random
from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

from papersearch.app.repository import Repo
from papersearch.adapters.feishu.notifier import FeishuNotifier


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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
