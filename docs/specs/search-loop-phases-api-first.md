# Search Loop Spec (API-First, Occam + First Principles)

## Context
This spec refines the 4-phase search loop with real data sources:
- Semantic Scholar APIs (discovery layer)
- Elsevier Research Products APIs (full-text XML for eligible content, esp. `10.1016/*`)

Guiding principles:
- **Occam's Razor**: smallest design that works end-to-end
- **First Principles**: optimize for machine-usable semantic text, not PDF accumulation

## Core Thesis
For AI-era literature workflows, **Markdown semantic streams** are more useful than PDF page containers.

- PDF: visually optimized, structurally noisy for model pipelines
- XML/JSON/MD: semantically explicit, chunkable, retrieval-friendly

## 4 Phases (Minimal, Implementable)

### Phase 1 — Basic Search (Candidate Discovery)
Input: user query

Process:
1. Query Semantic Scholar for candidate papers and references
2. Build candidate pool with DOI/title/abstract/citation links
3. If DOI prefix matches `10.1016/*`, mark for Elsevier full-text fetch

Output:
- Candidate list (deduped by DOI + normalized title)
- Candidate metadata with provenance (`semanticscholar`, `citation_expand`)

### Phase 2 — Relevance Classification (3-way)
Input: query + candidate textual representation

Text priority for each paper:
1. Elsevier XML-derived markdown (if available)
2. Other structured full text (future)
3. Abstract + key metadata fallback

Classifier output schema:
- `label`: `highly_relevant | closely_related | ignorable`
- `confidence`: 0..1
- `rationale`: short text
- `matched_constraints[]`
- `missing_constraints[]`

### Phase 3 — Adaptation & Exploration
Use phase-2 outputs to generate next-round queries:
- expand from `highly_relevant` (terms, methods, entities, refs)
- tighten from `closely_related` misses
- suppress drift using `ignorable` patterns

Next candidates come from:
- Semantic Scholar related/reference/citation expansion
- focused lexical/semantic reformulations

### Phase 4 — Comprehensiveness Estimation
Track round-by-round relevant discovery and stop when diminishing returns stabilize.

v1 stopping heuristic (minimal):
- stop if new `highly_relevant` < `m` for `r` consecutive rounds
  - default `m=2`, `r=2`

v2 estimator (after stable v1):
- fit saturation curve `R(n)=A*(1-exp(-n/tau))`
- report completeness `f=R/A`

## Round Contract
Each round stores:
- `round_id`
- `candidate_count`
- `classified_count`
- label distribution
- new highly relevant count
- cumulative highly relevant count
- stop decision + reason

## Why this is minimal but strong
- Only two external systems initially (S2 + Elsevier)
- No public endpoint required
- Works with current CLI + MCP adapter model
- Clear upgrade path to richer sources and convergence models
