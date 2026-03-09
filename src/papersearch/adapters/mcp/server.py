from __future__ import annotations

import json
import sys
from typing import Any

from papersearch.app.service import AppService
from papersearch.adapters.commands import run_command
from papersearch.adapters.feishu.notifier import FeishuNotifier


TOOLS: list[dict[str, Any]] = [
    {
        "name": "search_papers",
        "description": "Start a paper search job",
        "inputSchema": {
            "type": "object",
            "required": ["query"],
            "properties": {
                "query": {"type": "string", "minLength": 3},
                "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 20},
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "get_search_status",
        "description": "Get status for a search job",
        "inputSchema": {
            "type": "object",
            "required": ["search_id"],
            "properties": {"search_id": {"type": "string", "minLength": 1}},
            "additionalProperties": False,
        },
    },
    {
        "name": "get_search_results",
        "description": "List search results",
        "inputSchema": {
            "type": "object",
            "required": ["search_id"],
            "properties": {
                "search_id": {"type": "string", "minLength": 1},
                "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 20},
                "cursor": {"type": "string"},
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "create_collection",
        "description": "Create a collection",
        "inputSchema": {
            "type": "object",
            "required": ["name"],
            "properties": {
                "name": {"type": "string", "minLength": 1, "maxLength": 120},
                "description": {"type": "string", "maxLength": 2000},
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "add_paper_to_collection",
        "description": "Add paper to collection",
        "inputSchema": {
            "type": "object",
            "required": ["collection_id", "paper_id"],
            "properties": {
                "collection_id": {"type": "string", "minLength": 1},
                "paper_id": {"type": "string", "minLength": 1},
                "note": {"type": "string", "maxLength": 2000},
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "save_paper",
        "description": "Save paper optionally into collection",
        "inputSchema": {
            "type": "object",
            "required": ["paper_id"],
            "properties": {
                "paper_id": {"type": "string", "minLength": 1},
                "collection_id": {"type": "string"},
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "discover_candidates",
        "description": "Discover candidate papers from Semantic Scholar",
        "inputSchema": {
            "type": "object",
            "required": ["query"],
            "properties": {
                "query": {"type": "string", "minLength": 3},
                "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 10},
                "mock": {"type": "boolean", "default": False}
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "ingest_doi",
        "description": "Ingest DOI with Elsevier XML route + markdown render",
        "inputSchema": {
            "type": "object",
            "required": ["doi"],
            "properties": {
                "doi": {"type": "string", "minLength": 3},
                "title": {"type": "string"},
                "abstract": {"type": "string"},
                "mock": {"type": "boolean", "default": False},
                "fetch_assets": {"type": "boolean", "default": True}
            },
            "additionalProperties": False,
        },
    },
]


class MCPServer:
    def __init__(self):
        self.svc = AppService(notifier=FeishuNotifier.from_env())

    def handle(self, msg: dict[str, Any]) -> dict[str, Any] | None:
        req_id = msg.get("id")
        method = msg.get("method")
        params = msg.get("params", {})

        if method == "tools/list":
            return self._ok(req_id, {"tools": TOOLS})

        if method == "tools/call":
            name = params.get("name")
            args = params.get("arguments", {}) or {}
            return self._tool_call(req_id, name, args)

        if req_id is None:
            return None
        return self._err(req_id, -32601, f"Method not found: {method}")

    def _tool_call(self, req_id: Any, name: str, args: dict[str, Any]) -> dict[str, Any]:
        try:
            tool_to_command = {
                "search_papers": "search",
                "get_search_status": "search-status",
                "get_search_results": "search-results",
                "create_collection": "collection-create",
                "add_paper_to_collection": "collection-add",
                "save_paper": "save-paper",
                "discover_candidates": "discover",
                "ingest_doi": "ingest-doi",
            }
            command = tool_to_command.get(name or "")
            if not command:
                return self._ok(req_id, self._tool_error("INVALID_ARGUMENT", f"Unknown tool: {name}"))

            out = run_command(self.svc, command, args)
            return self._ok(req_id, {"content": [{"type": "text", "text": json.dumps(out, ensure_ascii=False)}], "isError": False})

        except ValueError as e:
            return self._ok(req_id, self._tool_error("INVALID_ARGUMENT", str(e)))
        except KeyError as e:
            return self._ok(req_id, self._tool_error("NOT_FOUND", str(e)))
        except Exception as e:
            return self._ok(req_id, self._tool_error("INTERNAL", str(e)))

    @staticmethod
    def _tool_error(code: str, message: str) -> dict[str, Any]:
        return {
            "content": [{"type": "text", "text": message}],
            "isError": True,
            "error": {"code": code, "message": message, "details": {}},
        }

    @staticmethod
    def _ok(req_id: Any, result: dict[str, Any]) -> dict[str, Any]:
        return {"jsonrpc": "2.0", "id": req_id, "result": result}

    @staticmethod
    def _err(req_id: Any, code: int, message: str) -> dict[str, Any]:
        return {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}


def main() -> int:
    server = MCPServer()
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
            resp = server.handle(msg)
            if resp is not None:
                sys.stdout.write(json.dumps(resp, ensure_ascii=False) + "\n")
                sys.stdout.flush()
        except Exception as e:
            sys.stdout.write(json.dumps({"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": str(e)}}) + "\n")
            sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
