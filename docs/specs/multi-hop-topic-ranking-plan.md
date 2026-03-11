# Multi-hop Expansion + Topic-layer Ranking Plan (v1)

Status: draft + phase-1 implementation started

## 1. Objective

Move from single-hop local graph lookup to a corpus-positioning engine that supports:

1. wider coverage (journal-scoped + frontier expansion)
2. multi-hop graph growth from seeds
3. topic-layer ranking on top of graph structure

## 2. Scope and constraints

- Local/private-first runtime remains unchanged.
- No public inbound endpoint.
- API-first ingestion for now; full-text optional and deferred.
- Prefer simple, testable, incremental implementation.

## 3. Architecture phases

### Phase A (implemented now): Graph expansion foundation

- Journal backfill via OpenAlex metadata/reference IDs
- Multi-hop expansion from seed set using missing reference DOIs
- Edge rebuild from DOI match + OpenAlex ID match

### Phase B: Graph relevance layer

- Personalized PageRank from seed set
- path distance / overlap features
- temporal priors (prior/derivative weighting)

### Phase C: Topic layer and hybrid ranker

- lexical retrieval (BM25 over title/abstract)
- dense retrieval (scientific embeddings)
- hybrid fusion (RRF or weighted fusion)
- optional diversification (MMR)

## 4. Data model requirements

`api_papers`
- `paper_id` (PK)
- `doi` (unique)
- `openalex_id` (nullable)
- `citation_count` (nullable)
- metadata fields (title/year/venue/abstract/source/updated_at)

`api_references`
- `src_paper_id`
- `ref_order`
- `doi` (nullable)
- `ref_openalex_id` (nullable)
- `raw_text`

`citation_edges`
- `src_paper_id`
- `dst_paper_id`
- `edge_source` in `doi_match|openalex_id_match|...`

## 5. API/CLI contracts

### 5.1 Journal backfill (already available)

`graph backfill-openalex-journal <journal> [--max-results N] [--per-page N]`

Returns:
- papers ingested
- refs ingested
- edge count
- graph stats

### 5.2 Multi-hop expansion (implemented in phase A)

`graph expand <seed1,seed2,...> [--rounds N] [--max-new-per-round N] [--max-workers N] [--mock]`

Algorithm:
1. resolve seeds to local paper IDs
2. from frontier papers, query unresolved reference DOIs
3. ingest candidate DOIs (bounded, parallel workers)
4. next frontier = newly ingested papers
5. repeat for N rounds

Return includes per-round summary: candidates/ingested/errors.

## 6. Ranking plan (next)

### 6.1 Graph-only ranking (P1)

- Personalized PageRank over local citation graph
- seed set as teleport distribution
- candidate explanations:
  - graph distance
  - direct seed edge counts
  - in/out edge evidence

Implementation status: initial `graph rank` CLI/MCP command implemented.

### 6.2 Hybrid ranking (P2)

Final score proposal:

`score = w1*bm25 + w2*embed_sim + w3*ppr + w4*coupling + w5*cocite + w6*time_decay`

Start with fixed weights; evaluate and tune.

## 7. Quality and evaluation

Offline evaluation packs:
- curated seed papers (domain experts)
- target relevant sets
- metrics: Recall@K, nDCG@K, edge coverage, expansion yield

Operational metrics:
- new papers per round
- API error rate per source
- unresolved DOI ratio
- edge density over time

## 8. Current implementation status

Implemented:
- OpenAlex single-journal backfill for metadata + refs (no full-text)
- schema fields for OpenAlex IDs and citation counts
- edge resolution using DOI and OpenAlex IDs
- multi-hop `graph expand` command and service logic

Pending next:
- checkpointed/resumable expansion
- PPR-based ranking endpoint
- hybrid lexical/embedding reranker
