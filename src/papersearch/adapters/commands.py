from __future__ import annotations

from typing import Any

from papersearch.app.service import AppService
from papersearch.ingest.pipeline import discover_candidates, ingest_doi


def run_command(svc: AppService, command: str, args: dict[str, Any]) -> dict[str, Any]:
    if command == "search":
        return svc.start_search(query=args["query"], limit=int(args.get("limit", 20)))
    if command == "search-status":
        return svc.get_search_status(args["search_id"])
    if command == "search-results":
        return svc.get_search_results(args["search_id"], limit=int(args.get("limit", 20)), cursor=args.get("cursor"))
    if command == "collection-create":
        return svc.create_collection(args["name"], description=args.get("description", ""))
    if command == "collection-add":
        return svc.add_paper_to_collection(args["collection_id"], args["paper_id"], note=args.get("note", ""))
    if command == "save-paper":
        return svc.save_paper(args["paper_id"], collection_id=args.get("collection_id"))
    if command == "discover":
        return {
            "query": args["query"],
            "items": discover_candidates(args["query"], limit=int(args.get("limit", 10)), use_mock=bool(args.get("mock", False))),
        }
    if command == "ingest-doi":
        return ingest_doi(
            args["doi"],
            title=args.get("title", ""),
            abstract=args.get("abstract", ""),
            use_mock=bool(args.get("mock", False)),
            fetch_assets=bool(args.get("fetch_assets", True)),
        )
    raise ValueError(f"unknown command: {command}")
