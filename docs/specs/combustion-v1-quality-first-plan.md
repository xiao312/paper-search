# Combustion v1 Plan (Quality-First, Minimal Scope)

Last updated: 2026-03-09

## 1) Scope Guardrails

This plan follows:
- **First Principles Thinking**: solve only irreducible core problems first.
- **Occam's Razor**: choose the simplest architecture that satisfies real workflows.

### In scope (v1)
1. Local corpus management for ~20k combustion PDFs
2. Citation/reference edge extraction and resolution
3. Fast CLI/MCP retrieval workflows on metadata + graph

### Out of scope (v1)
- Public service / multi-tenant SaaS
- Complex UI/visual map product
- Multi-graph ontology expansion (method/claim/question graphs)
- Heavy ranking/ML optimization before data quality is stable

---

## 2) First-Principles Product Requirements

### R1. Trustworthy paper records
- Every ingested file must map to one canonical paper record (or explicit failure state).
- Duplicate handling must be deterministic and auditable.

### R2. Trustworthy citation edges
- References should be extracted with DOI-first resolution.
- Edge confidence must be explicit (high/medium/low).

### R3. Practical retrieval
- Users/agents can find papers and graph neighbors quickly via CLI/MCP.
- Output must be machine-readable and stable.

---

## 3) Minimal Architecture (keep current stack)

- Storage: **SQLite** only
- Artifacts: normalized JSON + markdown on filesystem
- Interfaces: **CLI + MCP** only
- Adapters: no duplicated business logic
- Internal model: typed dataclasses + normalized schema contracts

No additional services/infra until KPIs justify.

---

## 4) Data Model (SQLite DDL v1)

```sql
CREATE TABLE IF NOT EXISTS papers (
  paper_id TEXT PRIMARY KEY,
  doi TEXT,
  title TEXT NOT NULL,
  year INTEGER,
  venue TEXT,
  abstract TEXT,
  source_pdf_path TEXT,
  source_hash TEXT NOT NULL,
  ingest_status TEXT NOT NULL, -- ok|partial|failed
  metadata_quality REAL NOT NULL DEFAULT 0,
  text_quality REAL NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(source_hash)
);

CREATE INDEX IF NOT EXISTS idx_papers_doi ON papers(doi);
CREATE INDEX IF NOT EXISTS idx_papers_year ON papers(year);

CREATE TABLE IF NOT EXISTS paper_authors (
  paper_id TEXT NOT NULL,
  author_order INTEGER NOT NULL,
  author_name TEXT NOT NULL,
  orcid TEXT,
  PRIMARY KEY (paper_id, author_order)
);

CREATE TABLE IF NOT EXISTS references_raw (
  ref_id TEXT PRIMARY KEY,
  source_paper_id TEXT NOT NULL,
  ref_order INTEGER,
  raw_text TEXT NOT NULL,
  doi TEXT,
  title TEXT,
  year INTEGER,
  resolved_paper_id TEXT,
  confidence TEXT NOT NULL DEFAULT 'low', -- high|medium|low
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_references_source ON references_raw(source_paper_id);
CREATE INDEX IF NOT EXISTS idx_references_doi ON references_raw(doi);
CREATE INDEX IF NOT EXISTS idx_references_resolved ON references_raw(resolved_paper_id);

CREATE TABLE IF NOT EXISTS citation_edges (
  src_paper_id TEXT NOT NULL,
  dst_paper_id TEXT NOT NULL,
  confidence TEXT NOT NULL, -- high|medium|low
  edge_source TEXT NOT NULL, -- doi_match|title_year_match|manual
  created_at TEXT NOT NULL,
  PRIMARY KEY (src_paper_id, dst_paper_id)
);

CREATE INDEX IF NOT EXISTS idx_edges_dst ON citation_edges(dst_paper_id);
```

Notes:
- Keep schema minimal; add columns only when needed by active features.
- `source_hash` is first dedup key; DOI is second-level identity signal.

---

## 5) Commands (CLI contract v1)

## 5.1 Library ingestion

### `papersearch library ingest-folder <path> [--glob "*.pdf"] [--limit N] [--dry-run]`
- Recursively scans PDFs
- Computes hash, extracts metadata/text, upserts canonical paper record
- Emits JSON summary:
  - scanned_count
  - ingested_ok
  - ingested_partial
  - failed_count
  - duplicate_hash_count
  - duplicate_doi_count

### `papersearch library stats`
- Returns corpus-level metrics:
  - total papers
  - doi coverage
  - metadata completeness buckets
  - average references/paper
  - resolved edge ratio

## 5.2 Paper retrieval

### `papersearch paper find --query <text> [--year-from Y] [--year-to Y] [--limit N]`
- Metadata-first retrieval (FTS/title/authors/venue)
- Deterministic sort policy documented

### `papersearch paper show <paper_id|doi>`
- Returns canonical metadata + provenance + quality metrics

## 5.3 Graph retrieval

### `papersearch graph neighbors --seed <paper_id|doi> [--direction out|in|both] [--hops 1] [--limit N]`
- Basic neighborhood expansion

### `papersearch graph related --seed <paper_id|doi> --mode cocite|coupling [--limit N]`
- v1 similarity using graph structure only

### `papersearch graph path-lite --from <paper_id|doi> --to <paper_id|doi> [--max-depth N]`
- Optional short-path-like query with bounded depth (no heavy algorithms required for v1)

---

## 6) MCP Tools (parity with CLI)

Expose equivalent MCP tools:
- `library_ingest_folder`
- `library_stats`
- `paper_find`
- `paper_show`
- `graph_neighbors`
- `graph_related`
- `graph_path_lite`

Parity rule:
- Same required fields
- Same error code schema
- Same stable JSON output shape

---

## 7) Implementation Map (repo modules)

- `src/papersearch/app/repository.py`
  - add paper/reference/edge tables + query methods
- `src/papersearch/app/service.py`
  - orchestrate ingest/report/find/graph calls
- `src/papersearch/ingest/`
  - add `ingest_local_pdf.py` (new)
  - reuse normalize/render/quality modules
  - add `resolve_references.py` (new)
- `src/papersearch/adapters/cli/main.py`
  - add `library`, `paper`, `graph` command groups
- `src/papersearch/adapters/mcp/server.py`
  - add matching tools via shared command layer
- `src/papersearch/adapters/commands.py`
  - single source of command dispatch logic

---

## 8) Quality Gates (must pass before feature expansion)

## 8.1 Per-paper gates
- has_title
- has_abstract_or_intro OR sufficient metadata fallback
- metadata_quality score
- text_quality score
- reference_count

## 8.2 Corpus gates
- ingest success rate
- DOI coverage
- duplicate collapse accuracy (spot-checked)
- resolved citation edge ratio

## 8.3 Reliability gates
- idempotent re-ingest behavior
- deterministic outputs for fixed input
- no silent exception swallowing in core path

---

## 9) Test Plan (required)

## 9.1 Unit tests
- hash/doi dedup rules
- reference DOI extraction and cleanup
- resolution priority (doi > title-year > unresolved)
- graph query correctness on synthetic fixtures

## 9.2 Contract tests
- CLI vs MCP parity for same inputs
- JSON schema stability for major commands

## 9.3 Smoke tests
- small local fixture folder (10–30 PDFs)
- ensure end-to-end ingest + graph query works offline

## 9.4 Benchmark harness (domain-specific)
- 20–30 known combustion seed papers
- evaluate:
  - retrieval usefulness
  - neighbor quality
  - latency

---

## 10) Milestones & Exit Criteria

## M1: Library Integrity
Deliver:
- `ingest-folder`, `library stats`, canonical paper table
Exit:
- >=90% ingest success on corpus
- deterministic reruns

## M2: Citation Integrity
Deliver:
- references extraction + citation_edges
Exit:
- stable edge construction
- acceptable resolved-edge ratio for internal use

## M3: Retrieval Utility
Deliver:
- `paper find/show`, `graph neighbors/related`
Exit:
- team can complete core literature workflows without manual spreadsheets

---

## 11) Anti-Checklist (what to avoid in this phase)
- Do not add new graph types without user-validated need.
- Do not introduce new databases/services prematurely.
- Do not optimize ranking before data quality baselines are met.
- Do not expand to other domains before combustion workflows are robust.

---

## 12) Immediate Next 7 Tasks
1. Add schema migration in `repository.py` for papers/references/edges.
2. Implement `ingest_local_pdf.py` with hash + metadata pipeline.
3. Implement `resolve_references.py` (DOI-first resolver).
4. Add CLI `library ingest-folder` + JSON summary.
5. Add CLI `paper find/show` commands.
6. Add CLI `graph neighbors/related` commands.
7. Add tests: dedup, resolver, CLI/MCP parity, fixture smoke.
