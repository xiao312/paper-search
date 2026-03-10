from __future__ import annotations

import argparse
import json
import sys

from papersearch.app.service import AppService
from papersearch.adapters.commands import run_command
from papersearch.adapters.feishu.notifier import FeishuNotifier


def _print_json(obj: dict):
    print(json.dumps(obj, ensure_ascii=False))


def _err(code: str, message: str, details=None):
    print(json.dumps({"code": code, "message": message, "details": details or {}}, ensure_ascii=False), file=sys.stderr)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="papersearch")
    sub = parser.add_subparsers(dest="command", required=True)

    s = sub.add_parser("search")
    s.add_argument("query")
    s.add_argument("--limit", type=int, default=20)
    s.add_argument("--json", action="store_true")

    s = sub.add_parser("search-status")
    s.add_argument("search_id")
    s.add_argument("--json", action="store_true")

    s = sub.add_parser("search-results")
    s.add_argument("search_id")
    s.add_argument("--limit", type=int, default=20)
    s.add_argument("--cursor")
    s.add_argument("--json", action="store_true")

    c = sub.add_parser("collection")
    c_sub = c.add_subparsers(dest="collection_cmd", required=True)

    c_create = c_sub.add_parser("create")
    c_create.add_argument("name")
    c_create.add_argument("--description", default="")
    c_create.add_argument("--json", action="store_true")

    c_add = c_sub.add_parser("add")
    c_add.add_argument("collection_id")
    c_add.add_argument("paper_id")
    c_add.add_argument("--note", default="")
    c_add.add_argument("--json", action="store_true")

    sp = sub.add_parser("save-paper")
    sp.add_argument("paper_id")
    sp.add_argument("--collection-id")
    sp.add_argument("--json", action="store_true")

    ds = sub.add_parser("discover")
    ds.add_argument("query")
    ds.add_argument("--limit", type=int, default=10)
    ds.add_argument("--mock", action="store_true")
    ds.add_argument("--json", action="store_true")

    bcs = sub.add_parser("bohrium-create-session")
    bcs.add_argument("query")
    bcs.add_argument("--sigma-model", default="auto")
    bcs.add_argument("--discipline", default="All")
    bcs.add_argument("--access-key")
    bcs.add_argument("--json", action="store_true")

    bsd = sub.add_parser("bohrium-session-detail")
    bsd.add_argument("uuid")
    bsd.add_argument("--access-key")
    bsd.add_argument("--json", action="store_true")

    bq = sub.add_parser("bohrium-question-papers")
    bq.add_argument("query_id")
    bq.add_argument("--sort", default="RelevanceScore")
    bq.add_argument("--access-key")
    bq.add_argument("--json", action="store_true")

    ig = sub.add_parser("ingest-doi")
    ig.add_argument("doi")
    ig.add_argument("--title", default="")
    ig.add_argument("--abstract", default="")
    ig.add_argument("--mock", action="store_true")
    ig.add_argument("--no-assets", action="store_true")
    ig.add_argument("--json", action="store_true")

    llm = sub.add_parser("llm")
    llm_sub = llm.add_subparsers(dest="llm_cmd", required=True)

    llm_list = llm_sub.add_parser("list-models")
    llm_list.add_argument("--provider")
    llm_list.add_argument("--search")
    llm_list.add_argument("--json", action="store_true")

    llm_prompt = llm_sub.add_parser("prompt")
    llm_prompt.add_argument("prompt")
    llm_prompt.add_argument("--provider")
    llm_prompt.add_argument("--model")
    llm_prompt.add_argument("--thinking")
    llm_prompt.add_argument("--json", action="store_true")

    sc = sub.add_parser("seed-candidates")
    sc.add_argument("query")
    sc.add_argument("query_id")
    sc.add_argument("--top-k", type=int, default=20)
    sc.add_argument("--sort", default="RelevanceScore")
    sc.add_argument("--provider")
    sc.add_argument("--model")
    sc.add_argument("--thinking")
    sc.add_argument("--json", action="store_true")

    sca = sub.add_parser("seed-candidates-auto")
    sca.add_argument("query")
    sca.add_argument("--top-k", type=int, default=20)
    sca.add_argument("--sort", default="RelevanceScore")
    sca.add_argument("--sigma-model", default="auto")
    sca.add_argument("--discipline", default="All")
    sca.add_argument("--provider")
    sca.add_argument("--model")
    sca.add_argument("--thinking")
    sca.add_argument("--wait-seconds", type=int, default=25)
    sca.add_argument("--poll-interval", type=float, default=1.5)
    sca.add_argument("--min-seed-count", type=int, default=5)
    sca.add_argument("--crossref-rows", type=int, default=30)
    sca.add_argument("--json", action="store_true")

    rc = sub.add_parser("relevance-classify-queryid")
    rc.add_argument("topic")
    rc.add_argument("query_id")
    rc.add_argument("--top-k", type=int, default=20)
    rc.add_argument("--sort", default="RelevanceScore")
    rc.add_argument("--provider", default="openai-codex")
    rc.add_argument("--model", default="gpt-5.1-codex-mini")
    rc.add_argument("--thinking", default="none")
    rc.add_argument("--max-workers", type=int, default=2)
    rc.add_argument("--json", action="store_true")

    g = sub.add_parser("graph")
    g_sub = g.add_subparsers(dest="graph_cmd", required=True)

    gi = g_sub.add_parser("ingest-doi")
    gi.add_argument("doi")
    gi.add_argument("--mock", action="store_true")
    gi.add_argument("--json", action="store_true")

    gs = g_sub.add_parser("stats")
    gs.add_argument("--json", action="store_true")

    gn = g_sub.add_parser("neighbors")
    gn.add_argument("seed")
    gn.add_argument("--direction", choices=["in", "out", "both"], default="both")
    gn.add_argument("--limit", type=int, default=50)
    gn.add_argument("--json", action="store_true")

    gr = g_sub.add_parser("related")
    gr.add_argument("seed")
    gr.add_argument("--mode", choices=["coupling", "cocite"], default="coupling")
    gr.add_argument("--limit", type=int, default=20)
    gr.add_argument("--json", action="store_true")

    gp = g_sub.add_parser("prior")
    gp.add_argument("seed")
    gp.add_argument("--direction", choices=["in", "out", "both"], default="both")
    gp.add_argument("--limit", type=int, default=50)
    gp.add_argument("--json", action="store_true")

    gd = g_sub.add_parser("derivative")
    gd.add_argument("seed")
    gd.add_argument("--direction", choices=["in", "out", "both"], default="both")
    gd.add_argument("--limit", type=int, default=50)
    gd.add_argument("--json", action="store_true")

    grs = g_sub.add_parser("related-set")
    grs.add_argument("seeds", help="Comma-separated DOI/paper_id seeds")
    grs.add_argument("--mode", choices=["coupling", "cocite"], default="coupling")
    grs.add_argument("--limit", type=int, default=20)
    grs.add_argument("--json", action="store_true")

    goj = g_sub.add_parser("ingest-openalex-journals")
    goj.add_argument("journals", help="Comma-separated journal names")
    goj.add_argument("--per-journal", type=int, default=10)
    goj.add_argument("--json", action="store_true")

    gbj = g_sub.add_parser("backfill-openalex-journal")
    gbj.add_argument("journal", help="Single journal name")
    gbj.add_argument("--max-results", type=int)
    gbj.add_argument("--per-page", type=int, default=200)
    gbj.add_argument("--json", action="store_true")

    ge = g_sub.add_parser("expand")
    ge.add_argument("seeds", help="Comma-separated DOI/paper_id seeds")
    ge.add_argument("--rounds", type=int, default=1)
    ge.add_argument("--max-new-per-round", type=int, default=100)
    ge.add_argument("--max-workers", type=int, default=2)
    ge.add_argument("--mock", action="store_true")
    ge.add_argument("--json", action="store_true")

    grank = g_sub.add_parser("rank")
    grank.add_argument("seeds", help="Comma-separated DOI/paper_id seeds")
    grank.add_argument("--limit", type=int, default=20)
    grank.add_argument("--alpha", type=float, default=0.85)
    grank.add_argument("--max-iter", type=int, default=100)
    grank.add_argument("--tol", type=float, default=1e-7)
    grank.add_argument("--include-seeds", action="store_true")
    grank.add_argument("--no-venue-prior", action="store_true")
    grank.add_argument("--same-venue-boost", type=float, default=0.20)
    grank.add_argument("--related-venue-boost", type=float, default=0.08)
    grank.add_argument("--json", action="store_true")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    svc = AppService(notifier=FeishuNotifier.from_env())

    try:
        if args.command == "collection":
            cmd = "collection-create" if args.collection_cmd == "create" else "collection-add"
            out = run_command(svc, cmd, vars(args))
        elif args.command == "ingest-doi":
            payload = vars(args).copy()
            payload["fetch_assets"] = not bool(args.no_assets)
            out = run_command(svc, "ingest-doi", payload)
        elif args.command == "llm":
            mapping = {
                "list-models": "llm-list-models",
                "prompt": "llm-prompt",
            }
            out = run_command(svc, mapping[args.llm_cmd], vars(args))
        elif args.command == "graph":
            mapping = {
                "ingest-doi": "graph-ingest-doi",
                "stats": "graph-stats",
                "neighbors": "graph-neighbors",
                "related": "graph-related",
                "prior": "graph-prior",
                "derivative": "graph-derivative",
                "related-set": "graph-related-set",
                "ingest-openalex-journals": "graph-ingest-openalex-journals",
                "backfill-openalex-journal": "graph-backfill-openalex-journal",
                "expand": "graph-expand",
                "rank": "graph-rank",
            }
            out = run_command(svc, mapping[args.graph_cmd], vars(args))
        else:
            out = run_command(svc, args.command, vars(args))

        _print_json(out)
        return 0

    except ValueError as e:
        _err("INVALID_ARGUMENT", str(e))
        return 2
    except KeyError as e:
        _err("NOT_FOUND", str(e))
        return 3
    except Exception as e:
        _err("INTERNAL", str(e))
        return 5


if __name__ == "__main__":
    raise SystemExit(main())
