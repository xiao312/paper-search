# Phase 1 Architecture (No Public Endpoint)

## Goal
Build an agent-ready paper search and organize system that runs locally/private-only:

- Core search + organize engine
- CLI interface (`--json` stable output)
- MCP tool server (local stdio transport)
- Feishu **outbound webhook notifier only**
- No inbound public callback URL

## Why this mode
- Fast iteration in early stage
- No public-domain ops burden
- Secure-by-default (no exposed callback endpoints)
- Keeps clean contracts for future API/public deployment

## Key References
- MCP tools contract (`tools/list`, `tools/call`, JSON Schema input/output)
- JSON-RPC 2.0 request/response/error semantics
- JSON Schema Draft 2020-12 as schema source of truth
- CLI Guidelines (`stdout` for machine output, `stderr` for logs/errors, `--json`)

## System Components

### 1) Core (pure business logic)
Responsibilities:
- Query parsing/intents
- Candidate retrieval (semantic + metadata + citation heuristics)
- Relevance ranking
- Convergence/completeness estimation
- Collection management

No transport logic (no CLI/MCP/HTTP specifics).

### 2) App Use Cases (orchestration)
Responsibilities:
- start_search
- get_search_status
- get_search_results
- create_collection
- add_paper_to_collection
- save_paper

These use cases are called by both CLI and MCP adapters.

### 3) CLI Adapter
Responsibilities:
- Human + script interface
- Stable machine output via `--json`
- Exit code contract

Rules:
- Structured result JSON -> `stdout`
- Errors/logs/progress -> `stderr`
- No parsing-critical text mixed into `stdout`

### 4) MCP Adapter
Responsibilities:
- Expose use cases as MCP tools
- Tool schemas from shared JSON Schema files
- Deterministic tool results + `isError` for execution failures

Transport for phase 1:
- `stdio` only (local agent integration)

### 5) Feishu Outbound Notifier (Webhook only)
Responsibilities:
- Send progress/complete notifications to Feishu custom bot webhook
- Optional card formatting
- Retry with backoff + idempotency guard

Out of scope in phase 1:
- Event subscription callbacks
- Message card callback handlers
- Any inbound public URL

## Runtime Topology (Phase 1)
- Local process A: CLI
- Local process B: MCP server
- Local process C: Worker/search jobs
- Outbound internet only: Feishu webhook POST

No inbound internet traffic required.

## Data and Job Model
- Search is asynchronous job:
  - `search_id`
  - status: `queued | running | completed | failed`
  - progress counters
  - completeness estimate
- Store:
  - metadata DB (start with sqlite)
  - vector index (local)

## Error Model
Use consistent typed errors across CLI + MCP:
- `INVALID_ARGUMENT`
- `NOT_FOUND`
- `CONFLICT`
- `RATE_LIMITED`
- `INTERNAL`

Include:
- `code`
- `message`
- `details` (optional)
- `correlation_id`

## Non-Goals (Phase 1)
- Public REST API service
- Multi-tenant authz/authn
- Full production observability stack
- Inbound Feishu bot interactions

## Upgrade Path to Phase 2
When needed:
1. Add HTTP API adapter (OpenAPI contract)
2. Add callback relay/tunnel (or public endpoint)
3. Reuse existing use cases + schemas (no business-logic rewrite)
