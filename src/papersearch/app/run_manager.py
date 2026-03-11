from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _stable_hash(obj: Any) -> str:
    b = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(b).hexdigest()


class RunManager:
    def __init__(self, base_dir: str = "docs/runs"):
        self.base = Path(base_dir)
        self.base.mkdir(parents=True, exist_ok=True)

    def start_run(self, query: str) -> dict:
        q = (query or "").strip()
        if len(q) < 3:
            raise ValueError("query must be at least 3 chars")
        run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}"
        run_dir = self.base / run_id
        run_dir.mkdir(parents=True, exist_ok=False)
        meta = {
            "run_id": run_id,
            "query": q,
            "created_at": _now_iso(),
            "updated_at": _now_iso(),
            "status": "started",
            "artifacts": {},
        }
        self.write_json(run_id, "meta.json", meta)
        return meta

    def run_dir(self, run_id: str) -> Path:
        p = self.base / run_id
        if not p.exists():
            raise KeyError("run not found")
        return p

    def read_json(self, run_id: str, filename: str) -> dict:
        p = self.run_dir(run_id) / filename
        if not p.exists():
            raise KeyError(f"artifact not found: {filename}")
        return json.loads(p.read_text(encoding="utf-8"))

    def write_json(self, run_id: str, filename: str, payload: dict) -> str:
        p = self.run_dir(run_id) / filename
        p.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        try:
            meta = self.read_json(run_id, "meta.json")
            meta["updated_at"] = _now_iso()
            meta.setdefault("artifacts", {})[filename] = {"path": str(p), "hash": _stable_hash(payload), "updated_at": _now_iso()}
            (self.run_dir(run_id) / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass
        return str(p)

    def append_event(
        self,
        run_id: str,
        op: str,
        status: str,
        input_payload: dict | None = None,
        output_file: str | None = None,
        summary: str | None = None,
        error: str | None = None,
        meta: dict | None = None,
    ) -> dict:
        event = {
            "event_id": f"evt_{uuid4().hex[:10]}",
            "ts": _now_iso(),
            "run_id": run_id,
            "op": op,
            "status": status,
            "input": input_payload or {},
            "input_hash": _stable_hash(input_payload or {}),
            "output_file": output_file,
            "summary": summary or "",
            "error": error,
            "meta": meta or {},
        }
        event["event_hash"] = _stable_hash(event)
        p = self.run_dir(run_id) / "history.jsonl"
        with p.open("a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
        return event

    def upsert_pool(self, run_id: str, papers: list[dict], source_op: str) -> dict:
        pool = self._try_read(run_id, "paper_pool.json") or {"papers": {}, "updates": []}
        rows = pool.setdefault("papers", {})
        added = 0
        updated = 0
        for p in papers or []:
            doi = (p.get("doi") or "").strip().lower()
            title = (p.get("title") or "").strip()
            pid = (p.get("paper_id") or (f"doi:{doi}" if doi else "")).strip().lower()
            if not pid and title:
                pid = f"title:{_stable_hash(title.lower())[:16]}"
            if not pid:
                continue

            now = _now_iso()
            prev = rows.get(pid)
            row = {
                "paper_id": pid,
                "doi": doi or (prev or {}).get("doi"),
                "title": title or (prev or {}).get("title") or "",
                "journal": (p.get("journal") or p.get("venue") or (prev or {}).get("journal") or ""),
                "publication_date": p.get("publication_date") or p.get("published") or (prev or {}).get("publication_date"),
                "abstract": p.get("abstract") or (prev or {}).get("abstract") or "",
                "full_text": p.get("full_text") or (prev or {}).get("full_text") or "",
                "relevance_score": p.get("relevance_score") if p.get("relevance_score") is not None else (prev or {}).get("relevance_score"),
                "sort_score": p.get("sort_score") if p.get("sort_score") is not None else (prev or {}).get("sort_score"),
                "source": p.get("source") or (prev or {}).get("source") or source_op,
                "first_seen_at": (prev or {}).get("first_seen_at") or now,
                "last_seen_at": now,
                "seen_in_ops": sorted(list(set(((prev or {}).get("seen_in_ops") or []) + [source_op]))),
            }
            rows[pid] = row
            if prev is None:
                added += 1
            else:
                updated += 1

        pool.setdefault("updates", []).append({"ts": _now_iso(), "source_op": source_op, "added": added, "updated": updated, "pool_size": len(rows)})
        self.write_json(run_id, "paper_pool.json", pool)
        return {"added": added, "updated": updated, "pool_size": len(rows)}

    def list_pool_papers(self, run_id: str) -> list[dict]:
        pool = self._try_read(run_id, "paper_pool.json") or {}
        rows = list((pool.get("papers") or {}).values())
        rows.sort(key=lambda x: (x.get("first_seen_at") or "", x.get("paper_id") or ""))
        return rows

    def compile_report(self, run_id: str) -> dict:
        query = self.read_json(run_id, "meta.json").get("query")
        search = self._try_read(run_id, "search.json")
        classify = self._try_read(run_id, "classify.json")
        grow = self._try_read(run_id, "grow.json")
        rank = self._try_read(run_id, "rank.json")
        scored = self._try_read(run_id, "pool_scored.json") or {"items": []}
        perf = self._try_read(run_id, "perf.json") or {}
        pool = self._try_read(run_id, "paper_pool.json") or {"papers": {}}

        by_label = {"highly_relevant": [], "closely_related": [], "ignorable": [], "non_classifiable": []}
        for it in (classify or {}).get("items", []) or []:
            lbl = (it.get("label") or "non_classifiable").strip()
            by_label.setdefault(lbl, []).append(it)

        source_counts: dict[str, int] = {}
        for p in (search or {}).get("new_papers", []) or []:
            src = (p.get("source") or "unknown").strip() or "unknown"
            source_counts[src] = source_counts.get(src, 0) + 1

        lines = [
            "# Query",
            "",
            query or "",
            "",
            "# Search Results (New Papers)",
            "",
            f"- total_new_papers: {len((search or {}).get('new_papers', []) or [])}",
            f"- source_counts: {source_counts}",
            "",
        ]
        for p in (search or {}).get("new_papers", []) or []:
            title = p.get("title") or ""
            doi = p.get("doi") or ""
            src = p.get("source") or ""
            lines.append(f"- {title} ({doi}) [{src}]")

        lines.extend(["", "# Paper Pool", "", f"- pool_size: {len((pool.get('papers') or {}))}"])

        lines.extend(["", "# Relevance Classification", ""])
        for sec, title in [
            ("highly_relevant", "Highly Relevant"),
            ("closely_related", "Closely Related"),
            ("ignorable", "Ignorable"),
            ("non_classifiable", "Non Classifiable"),
        ]:
            items = by_label.get(sec, [])
            lines.extend([f"## {title} ({len(items)})", ""])
            if not items:
                lines.append("- (none)")
            for it in items:
                reason = (it.get("reason") or "").strip()
                if reason:
                    lines.append(f"- {(it.get('title') or '')} ({it.get('doi') or ''}) — {reason}")
                else:
                    lines.append(f"- {(it.get('title') or '')} ({it.get('doi') or ''})")

        lines.extend(["", "# Growth Findings (2-hop)", ""])
        for lv in (grow or {}).get("results", []) or []:
            lines.append(f"- Level {lv.get('level')}: discovered {lv.get('discovered_count')} papers")

        lines.extend(["", "# Ranking Summary", ""])
        for it in (rank or {}).get("items", [])[:20]:
            lines.append(f"- {it.get('title') or ''} ({it.get('doi') or ''}) score={it.get('score')}")

        scored_items = list((scored or {}).get("items") or [])
        top_influential = scored_items[:10]
        top_relevant_influential = [x for x in scored_items if x.get("classification_label") in ("highly_relevant", "closely_related")][:10]
        legacy_core = [x for x in scored_items if x.get("year") is not None]
        legacy_core.sort(key=lambda x: ((x.get("year") or 9999), -(x.get("influence_score") or 0.0)))
        legacy_core = legacy_core[:10]

        rising_recent = [x for x in scored_items if x.get("year") is not None and x.get("classification_label") in ("highly_relevant", "closely_related")]
        rising_recent.sort(key=lambda x: ((x.get("year") or 0), (x.get("influence_score") or 0.0)), reverse=True)
        rising_recent = rising_recent[:10]

        lines.extend(["", "# Influence Scoring (Year + Citations + PageRank + Relevance)", ""])
        lines.append(f"- scored_papers: {len(scored_items)}")
        lines.append(f"- weights: {(scored or {}).get('weights') or {}}")

        lines.extend(["", "## Top Influential Papers", ""])
        if not top_influential:
            lines.append("- (none)")
        for it in top_influential:
            lines.append(
                f"- {it.get('title') or ''} ({it.get('doi') or ''}) influence={it.get('influence_score')} "
                f"label={it.get('classification_label')} pagerank={it.get('pagerank_score')} cites={it.get('citation_count')} year={it.get('year')}"
            )

        lines.extend(["", "## Top Influential (Relevant Only)", ""])
        if not top_relevant_influential:
            lines.append("- (none)")
        for it in top_relevant_influential:
            lines.append(
                f"- {it.get('title') or ''} ({it.get('doi') or ''}) influence={it.get('influence_score')} "
                f"label={it.get('classification_label')} pagerank={it.get('pagerank_score')} cites={it.get('citation_count')} year={it.get('year')}"
            )

        lines.extend(["", "## Legacy Core Nodes (Older but Influential)", ""])
        if not legacy_core:
            lines.append("- (none)")
        for it in legacy_core:
            lines.append(
                f"- {it.get('title') or ''} ({it.get('doi') or ''}) year={it.get('year')} "
                f"influence={it.get('influence_score')} cites={it.get('citation_count')} pagerank={it.get('pagerank_score')}"
            )

        lines.extend(["", "## Rising Nodes (Recent + Relevant + Influential)", ""])
        if not rising_recent:
            lines.append("- (none)")
        for it in rising_recent:
            lines.append(
                f"- {it.get('title') or ''} ({it.get('doi') or ''}) year={it.get('year')} "
                f"influence={it.get('influence_score')} label={it.get('classification_label')} pagerank={it.get('pagerank_score')}"
            )

        lines.extend(["", "# Performance Diagnostics", ""])
        stage_ms = (perf or {}).get("stage_durations_ms") or {}
        if not stage_ms:
            lines.append("- (no perf diagnostics captured yet)")
        else:
            for op, ms in sorted(stage_ms.items(), key=lambda x: x[1], reverse=True):
                lines.append(f"- {op}: {ms} ms")

        sugg = (perf or {}).get("speedup_suggestions") or []
        lines.extend(["", "## Speedup Suggestions", ""])
        if not sugg:
            lines.append("- (none)")
        for s in sugg:
            lines.append(f"- {s}")

        lines.extend(["", "# Method & Provenance", "", "- Operations: search -> classify -> grow -> rank -> score -> diagnostics", "- Artifacts are stored as JSON sidecars in this run directory.", "", "# Reproducibility", "", "- See history.jsonl for append-only operation trace."])

        md = "\n".join(lines).strip() + "\n"
        report_json = {
            "run_id": run_id,
            "query": query,
            "search": search,
            "classification": classify,
            "grow": grow,
            "rank": rank,
            "scored": scored,
            "perf": perf,
            "paper_pool": pool,
            "summary": {
                "highly_relevant": len(by_label.get("highly_relevant", [])),
                "closely_related": len(by_label.get("closely_related", [])),
                "ignorable": len(by_label.get("ignorable", [])),
                "non_classifiable": len(by_label.get("non_classifiable", [])),
                "search_source_counts": source_counts,
                "pool_size": len((pool.get("papers") or {})),
                "scored_items": len((scored or {}).get("items") or []),
                "top_influential": [
                    {
                        "paper_id": it.get("paper_id"),
                        "doi": it.get("doi"),
                        "title": it.get("title"),
                        "influence_score": it.get("influence_score"),
                        "classification_label": it.get("classification_label"),
                    }
                    for it in ((scored or {}).get("items") or [])[:10]
                ],
                "top_bottleneck": (((perf or {}).get("top_bottlenecks") or [{}])[0] if (perf or {}).get("top_bottlenecks") else None),
            },
        }

        (self.run_dir(run_id) / "report.md").write_text(md, encoding="utf-8")
        self.write_json(run_id, "report.json", report_json)
        return {"run_id": run_id, "report_md": str(self.run_dir(run_id) / "report.md"), "report_json": str(self.run_dir(run_id) / "report.json")}

    def _try_read(self, run_id: str, filename: str) -> dict | None:
        try:
            return self.read_json(run_id, filename)
        except Exception:
            return None
