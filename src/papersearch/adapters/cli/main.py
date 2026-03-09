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

    ig = sub.add_parser("ingest-doi")
    ig.add_argument("doi")
    ig.add_argument("--title", default="")
    ig.add_argument("--abstract", default="")
    ig.add_argument("--mock", action="store_true")
    ig.add_argument("--no-assets", action="store_true")
    ig.add_argument("--json", action="store_true")

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
