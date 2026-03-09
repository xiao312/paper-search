# Careful Plan Roadmap (No Public Endpoint)

## Product Direction
Agent-ready literature system with local/private deployment:
- CLI + MCP as primary interfaces
- outbound-only Feishu notifications
- API-first ingestion using existing services (Semantic Scholar + Elsevier)

## Phase Plan

## Phase A — Foundation (current)
- Local DB + deterministic search loop scaffolding
- CLI contract + MCP tools contract
- Feishu outbound webhook notifier

Exit criteria:
- end-to-end local run works
- machine-readable outputs stable

## Phase B — Real Discovery + Ingestion
- Integrate Semantic Scholar discovery API
- DOI routing to Elsevier full-text fetch for `10.1016/*`
- XML -> normalized JSON -> markdown pipeline

Exit criteria:
- at least 70% of target test DOI set ingests to quality threshold
- markdown output parse quality accepted

## Phase C — Relevance + Adaptation Loop
- LLM 3-way relevance classification on markdown-first context
- iterative adaptation/exploration with citation + semantic expansion
- round metrics and stop reasons

Exit criteria:
- measurable gains in highly-relevant discovery across rounds
- low drift and explainable loop decisions

## Phase D — Comprehensiveness Estimation
- v1 heuristic stop (diminishing returns)
- v2 optional exponential convergence model
- expose completeness estimate via CLI + MCP

Exit criteria:
- stable stop behavior across representative queries
- completeness estimate included in outputs

## API/Service Usage Matrix

| Capability | Service | Why |
|---|---|---|
| Discovery | Semantic Scholar API | broad, fast metadata + graph |
| Eligible full text | Elsevier APIs | structured XML for machine parsing |
| Notification | Feishu custom bot webhook | outbound-only, no public endpoint |
| Agent interface | MCP (stdio) | local tool-calling integration |
| Human/script interface | CLI | stable automation + debugging |

## Risk Controls
- Entitlement/IP dependency for Elsevier full text -> explicit fallback to abstract-only mode
- XML variability -> parser profile + quality checks + warning flags
- API limits -> cache + retry policy + throttling
- LLM uncertainty -> strict output schema + confidence-aware handling

## Minimalism Checklist (Occam)
- no public HTTP service in current stage
- no duplicated business logic in adapters
- one canonical normalized document format
- one iterative search loop orchestrator
- add complexity only when metrics justify
