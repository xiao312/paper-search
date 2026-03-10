from __future__ import annotations

from typing import Any

from papersearch.app.service import AppService
from papersearch.ingest.pipeline import discover_candidates, ingest_doi


def run_command(svc: AppService, command: str, args: dict[str, Any]) -> dict[str, Any]:
    if command == "search":
        return svc.op_search(
            prompt=str(args["query"]),
            top_k=int(args.get("limit", 20)),
            min_seed_count=max(5, min(int(args.get("limit", 20)), 50)),
            crossref_rows=30,
        )
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
    if command == "bohrium-create-session":
        return svc.bohrium_create_session(
            query=str(args["query"]),
            model=str(args.get("sigma_model", "auto")),
            discipline=str(args.get("discipline", "All")),
            resource_id_list=list(args.get("resource_id_list") or []),
            access_key=args.get("access_key"),
        )
    if command == "bohrium-session-detail":
        return svc.bohrium_session_detail(uuid=str(args["uuid"]), access_key=args.get("access_key"))
    if command == "bohrium-question-papers":
        return svc.bohrium_question_papers(
            query_id=str(args["query_id"]),
            sort=str(args.get("sort", "RelevanceScore")),
            access_key=args.get("access_key"),
        )
    if command == "llm-list-models":
        return svc.llm_list_models(provider=args.get("provider"), search=args.get("search"))
    if command == "llm-prompt":
        return svc.llm_prompt(
            prompt=str(args["prompt"]),
            provider=args.get("provider"),
            model=args.get("model"),
            thinking=args.get("thinking"),
        )
    if command == "seed-candidates":
        return svc.seed_candidates_from_query_id(
            query=str(args["query"]),
            query_id=str(args["query_id"]),
            top_k=int(args.get("top_k", 20)),
            sort=str(args.get("sort", "RelevanceScore")),
            provider=args.get("provider"),
            model=args.get("model"),
            thinking=args.get("thinking"),
        )
    if command == "seed-candidates-auto":
        return svc.seed_candidates(
            query=str(args["query"]),
            top_k=int(args.get("top_k", 20)),
            sort=str(args.get("sort", "RelevanceScore")),
            provider=args.get("provider"),
            model=args.get("model"),
            thinking=args.get("thinking"),
            sigma_model=str(args.get("sigma_model", "auto")),
            discipline=str(args.get("discipline", "All")),
            wait_seconds=int(args.get("wait_seconds", 25)),
            poll_interval=float(args.get("poll_interval", 1.5)),
            min_seed_count=int(args.get("min_seed_count", 5)),
            crossref_rows=int(args.get("crossref_rows", 30)),
        )
    if command == "relevance-classify-queryid":
        return svc.relevance_classify_query_id(
            topic=str(args["topic"]),
            query_id=str(args["query_id"]),
            top_k=int(args.get("top_k", 20)),
            sort=str(args.get("sort", "RelevanceScore")),
            provider=args.get("provider", "zai"),
            model=args.get("model", "glm-4.5-flash"),
            thinking=args.get("thinking", "off"),
            max_workers=int(args.get("max_workers", 2)),
        )
    if command == "op-search":
        return svc.op_search(
            prompt=str(args["prompt"]),
            top_k=int(args.get("top_k", 20)),
            min_seed_count=int(args.get("min_seed_count", 5)),
            crossref_rows=int(args.get("crossref_rows", 30)),
            sort=str(args.get("sort", "RelevanceScore")),
            provider=args.get("provider"),
            model=args.get("model"),
            thinking=args.get("thinking"),
            sigma_model=str(args.get("sigma_model", "auto")),
            discipline=str(args.get("discipline", "ET")),
            wait_seconds=int(args.get("wait_seconds", 30)),
            poll_interval=float(args.get("poll_interval", 1.5)),
        )
    if command == "op-classify":
        return svc.op_classify(
            topic=str(args["topic"]),
            query_id=str(args["query_id"]),
            top_k=int(args.get("top_k", 20)),
            sort=str(args.get("sort", "RelevanceScore")),
            provider=args.get("provider", "zai"),
            model=args.get("model", "glm-4.5-flash"),
            thinking=args.get("thinking", "off"),
            max_workers=int(args.get("max_workers", 2)),
        )
    if command == "op-grow":
        seeds_raw = args.get("seeds", "")
        seeds = [s.strip() for s in str(seeds_raw).split(",") if s.strip()]
        return svc.op_grow(
            seeds=seeds,
            levels=int(args.get("levels", 2)),
            limit_per_node=int(args.get("limit_per_node", 30)),
            use_mock=bool(args.get("mock", False)),
        )
    if command == "ingest-doi":
        return ingest_doi(
            args["doi"],
            title=args.get("title", ""),
            abstract=args.get("abstract", ""),
            use_mock=bool(args.get("mock", False)),
            fetch_assets=bool(args.get("fetch_assets", True)),
        )
    if command == "graph-ingest-doi":
        return svc.graph_ingest_doi(args["doi"], use_mock=bool(args.get("mock", False)))
    if command == "graph-stats":
        return svc.graph_stats()
    if command == "graph-neighbors":
        return svc.graph_neighbors(args["seed"], direction=args.get("direction", "both"), limit=int(args.get("limit", 50)))
    if command == "graph-related":
        return svc.graph_related(args["seed"], mode=args.get("mode", "coupling"), limit=int(args.get("limit", 20)))
    if command == "graph-prior":
        return svc.graph_prior(args["seed"], direction=args.get("direction", "both"), limit=int(args.get("limit", 50)))
    if command == "graph-derivative":
        return svc.graph_derivative(args["seed"], direction=args.get("direction", "both"), limit=int(args.get("limit", 50)))
    if command == "graph-related-set":
        seeds_raw = args.get("seeds", "")
        if isinstance(seeds_raw, str):
            seeds = [s.strip() for s in seeds_raw.split(",") if s.strip()]
        else:
            seeds = list(seeds_raw or [])
        return svc.graph_related_set(seeds, mode=args.get("mode", "coupling"), limit=int(args.get("limit", 20)))
    if command == "graph-ingest-openalex-journals":
        journals_raw = args.get("journals", "")
        journals = [s.strip() for s in journals_raw.split(",") if s.strip()]
        return svc.graph_ingest_openalex_journals(journals=journals, per_journal=int(args.get("per_journal", 10)))
    if command == "graph-backfill-openalex-journal":
        max_results = args.get("max_results")
        return svc.graph_backfill_openalex_journal(
            journal=args["journal"],
            max_results=int(max_results) if max_results else None,
            per_page=int(args.get("per_page", 200)),
        )
    if command == "graph-expand":
        seeds_raw = args.get("seeds", "")
        seeds = [s.strip() for s in seeds_raw.split(",") if s.strip()]
        return svc.graph_expand(
            seeds=seeds,
            rounds=int(args.get("rounds", 1)),
            max_new_per_round=int(args.get("max_new_per_round", 100)),
            use_mock=bool(args.get("mock", False)),
            max_workers=int(args.get("max_workers", 2)),
        )
    if command == "graph-rank":
        seeds_raw = args.get("seeds", "")
        seeds = [s.strip() for s in seeds_raw.split(",") if s.strip()]
        return svc.graph_rank(
            seeds=seeds,
            limit=int(args.get("limit", 20)),
            alpha=float(args.get("alpha", 0.85)),
            max_iter=int(args.get("max_iter", 100)),
            tol=float(args.get("tol", 1e-7)),
            include_seeds=bool(args.get("include_seeds", False)),
            venue_prior=not bool(args.get("no_venue_prior", False)),
            same_venue_boost=float(args.get("same_venue_boost", 0.20)),
            related_venue_boost=float(args.get("related_venue_boost", 0.08)),
        )
    raise ValueError(f"unknown command: {command}")
