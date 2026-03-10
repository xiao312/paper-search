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
        "name": "bohrium_create_session",
        "description": "Create Bohrium sigma-search session from query",
        "inputSchema": {
            "type": "object",
            "required": ["query"],
            "properties": {
                "query": {"type": "string", "minLength": 3},
                "sigma_model": {"type": "string", "default": "auto"},
                "discipline": {"type": "string", "default": "All"},
                "access_key": {"type": "string"}
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "bohrium_session_detail",
        "description": "Get Bohrium sigma-search session detail by uuid",
        "inputSchema": {
            "type": "object",
            "required": ["uuid"],
            "properties": {
                "uuid": {"type": "string", "minLength": 1},
                "access_key": {"type": "string"}
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "bohrium_question_papers",
        "description": "Get papers for a Bohrium Sigma-search query ID",
        "inputSchema": {
            "type": "object",
            "required": ["query_id"],
            "properties": {
                "query_id": {"type": "string", "minLength": 1},
                "sort": {"type": "string", "default": "RelevanceScore"},
                "access_key": {"type": "string"}
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "llm_list_models",
        "description": "List models from pi-mono provider registry via installed pi CLI",
        "inputSchema": {
            "type": "object",
            "properties": {
                "provider": {"type": "string"},
                "search": {"type": "string"}
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "llm_prompt",
        "description": "Run a single-shot pi prompt with provider/model selection",
        "inputSchema": {
            "type": "object",
            "required": ["prompt"],
            "properties": {
                "prompt": {"type": "string", "minLength": 1},
                "provider": {"type": "string"},
                "model": {"type": "string"},
                "thinking": {"type": "string"}
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "seed_candidates",
        "description": "Use pi LLM to expand a query and Bohrium queryID papers to select seed candidates",
        "inputSchema": {
            "type": "object",
            "required": ["query", "query_id"],
            "properties": {
                "query": {"type": "string", "minLength": 3},
                "query_id": {"type": "string", "minLength": 1},
                "top_k": {"type": "integer", "minimum": 1, "maximum": 200, "default": 20},
                "sort": {"type": "string", "default": "RelevanceScore"},
                "provider": {"type": "string"},
                "model": {"type": "string"},
                "thinking": {"type": "string"}
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "seed_candidates_auto",
        "description": "Create Bohrium session, resolve queryID, then select seed candidates",
        "inputSchema": {
            "type": "object",
            "required": ["query"],
            "properties": {
                "query": {"type": "string", "minLength": 3},
                "top_k": {"type": "integer", "minimum": 1, "maximum": 200, "default": 20},
                "sort": {"type": "string", "default": "RelevanceScore"},
                "sigma_model": {"type": "string", "default": "auto"},
                "discipline": {"type": "string", "default": "All"},
                "provider": {"type": "string"},
                "model": {"type": "string"},
                "thinking": {"type": "string"},
                "wait_seconds": {"type": "integer", "minimum": 0, "default": 25},
                "poll_interval": {"type": "number", "exclusiveMinimum": 0, "default": 1.5},
                "min_seed_count": {"type": "integer", "minimum": 1, "maximum": 200, "default": 5},
                "crossref_rows": {"type": "integer", "minimum": 1, "maximum": 100, "default": 30}
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "relevance_classify_queryid",
        "description": "Batch classify papers into highly_relevant/closely_related/ignorable (or non_classifiable) using abstracts",
        "inputSchema": {
            "type": "object",
            "required": ["topic", "query_id"],
            "properties": {
                "topic": {"type": "string", "minLength": 3},
                "query_id": {"type": "string", "minLength": 1},
                "top_k": {"type": "integer", "minimum": 1, "maximum": 200, "default": 20},
                "sort": {"type": "string", "default": "RelevanceScore"},
                "provider": {"type": "string", "default": "openai-codex"},
                "model": {"type": "string", "default": "gpt-5.1-codex-mini"},
                "thinking": {"type": "string", "default": "none"},
                "max_workers": {"type": "integer", "minimum": 1, "maximum": 8, "default": 2}
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
    {
        "name": "graph_ingest_doi",
        "description": "Ingest DOI into local citation graph store and rebuild DOI-matched edges",
        "inputSchema": {
            "type": "object",
            "required": ["doi"],
            "properties": {
                "doi": {"type": "string", "minLength": 3},
                "mock": {"type": "boolean", "default": False}
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "graph_stats",
        "description": "Get local citation graph stats",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "additionalProperties": False,
        },
    },
    {
        "name": "graph_neighbors",
        "description": "Get incoming/outgoing citation neighbors for a seed DOI or paper_id",
        "inputSchema": {
            "type": "object",
            "required": ["seed"],
            "properties": {
                "seed": {"type": "string", "minLength": 1},
                "direction": {"type": "string", "enum": ["in", "out", "both"], "default": "both"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 200, "default": 50}
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "graph_related",
        "description": "Get related papers by citation-structure overlap (coupling/cocite)",
        "inputSchema": {
            "type": "object",
            "required": ["seed"],
            "properties": {
                "seed": {"type": "string", "minLength": 1},
                "mode": {"type": "string", "enum": ["coupling", "cocite"], "default": "coupling"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 200, "default": 20}
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "graph_prior",
        "description": "Get prior-work neighbors (older than seed year)",
        "inputSchema": {
            "type": "object",
            "required": ["seed"],
            "properties": {
                "seed": {"type": "string", "minLength": 1},
                "direction": {"type": "string", "enum": ["in", "out", "both"], "default": "both"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 200, "default": 50}
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "graph_derivative",
        "description": "Get derivative-work neighbors (newer than seed year)",
        "inputSchema": {
            "type": "object",
            "required": ["seed"],
            "properties": {
                "seed": {"type": "string", "minLength": 1},
                "direction": {"type": "string", "enum": ["in", "out", "both"], "default": "both"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 200, "default": 50}
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "graph_related_set",
        "description": "Get related papers from a seed set (comma-separated seeds)",
        "inputSchema": {
            "type": "object",
            "required": ["seeds"],
            "properties": {
                "seeds": {"type": "string", "minLength": 1},
                "mode": {"type": "string", "enum": ["coupling", "cocite"], "default": "coupling"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 200, "default": 20}
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "graph_ingest_openalex_journals",
        "description": "Ingest papers from OpenAlex by journal names (comma-separated)",
        "inputSchema": {
            "type": "object",
            "required": ["journals"],
            "properties": {
                "journals": {"type": "string", "minLength": 1},
                "per_journal": {"type": "integer", "minimum": 1, "maximum": 100, "default": 10}
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "graph_backfill_openalex_journal",
        "description": "Backfill one journal from OpenAlex with refs/citation metadata only",
        "inputSchema": {
            "type": "object",
            "required": ["journal"],
            "properties": {
                "journal": {"type": "string", "minLength": 1},
                "max_results": {"type": "integer", "minimum": 1},
                "per_page": {"type": "integer", "minimum": 1, "maximum": 200, "default": 200}
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "graph_expand",
        "description": "Multi-hop expansion from seed papers using missing reference DOIs",
        "inputSchema": {
            "type": "object",
            "required": ["seeds"],
            "properties": {
                "seeds": {"type": "string", "minLength": 1},
                "rounds": {"type": "integer", "minimum": 1, "maximum": 10, "default": 1},
                "max_new_per_round": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 100},
                "max_workers": {"type": "integer", "minimum": 1, "maximum": 8, "default": 2},
                "mock": {"type": "boolean", "default": false}
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "graph_rank",
        "description": "Rank related papers using personalized PageRank over local citation graph",
        "inputSchema": {
            "type": "object",
            "required": ["seeds"],
            "properties": {
                "seeds": {"type": "string", "minLength": 1},
                "limit": {"type": "integer", "minimum": 1, "maximum": 200, "default": 20},
                "alpha": {"type": "number", "exclusiveMinimum": 0, "exclusiveMaximum": 1, "default": 0.85},
                "max_iter": {"type": "integer", "minimum": 1, "maximum": 200, "default": 100},
                "tol": {"type": "number", "exclusiveMinimum": 0, "default": 1e-7},
                "include_seeds": {"type": "boolean", "default": false},
                "no_venue_prior": {"type": "boolean", "default": false},
                "same_venue_boost": {"type": "number", "minimum": 0, "default": 0.2},
                "related_venue_boost": {"type": "number", "minimum": 0, "default": 0.08}
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
                "bohrium_create_session": "bohrium-create-session",
                "bohrium_session_detail": "bohrium-session-detail",
                "bohrium_question_papers": "bohrium-question-papers",
                "llm_list_models": "llm-list-models",
                "llm_prompt": "llm-prompt",
                "seed_candidates": "seed-candidates",
                "seed_candidates_auto": "seed-candidates-auto",
                "relevance_classify_queryid": "relevance-classify-queryid",
                "ingest_doi": "ingest-doi",
                "graph_ingest_doi": "graph-ingest-doi",
                "graph_stats": "graph-stats",
                "graph_neighbors": "graph-neighbors",
                "graph_related": "graph-related",
                "graph_prior": "graph-prior",
                "graph_derivative": "graph-derivative",
                "graph_related_set": "graph-related-set",
                "graph_ingest_openalex_journals": "graph-ingest-openalex-journals",
                "graph_backfill_openalex_journal": "graph-backfill-openalex-journal",
                "graph_expand": "graph-expand",
                "graph_rank": "graph-rank",
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
