# Citation/Reference Graph Exploration Plan (Connected Papers / ResearchRabbit-inspired)

Last updated: 2026-03-09

## 1) What users actually do in these tools (from docs/guides)

Common behavior patterns observed:

1. **Start from a seed paper**
   - Users already know 1-3 key papers and want a map around them.

2. **Ask 3 practical questions repeatedly**
   - What are similar papers?
   - What are important prior works (foundational)?
   - What are derivative works (newer follow-ups)?

3. **Iterate quickly**
   - click a node, expand neighborhood, keep/reject candidates.

4. **Use collection-centric workflow**
   - maintain a set of selected papers and refine recommendations.

5. **Need timeline/context**
   - understand development trajectory (older -> newer).

6. **Value "keep me updated"**
   - alerts/monitoring are useful, but can be phase-2.

### Source references used for this scan
- Connected Papers about page: https://www.connectedpapers.com/about
- Connected Papers explainer/articles: https://medium.com/connectedpapers/announcing-connected-papers-a-visual-tool-for-researchers-to-find-and-explore-academic-papers-89146a54c7d4
- ResearchRabbit guide/help pages:
  - https://www.researchrabbit.ai/help/guide
  - https://www.researchrabbit.ai/articles/guide-to-using-researchrabbit
- Litmaps usage references:
  - https://www.litmaps.com/about/for-researchers
  - https://libguides.hkust.edu.hk/citation-chaining/litmaps


## 2) Product decision for this repo (Occam + First Principles)

We should **not** build full visualization product now.

We should build a **graph retrieval core** that supports seed-based exploration with stable CLI/MCP contracts.

### v1 objective
Deliver high-utility exploration through APIs/CLI using citation/reference structure only.

### v1 non-goals
- no heavy UI map
- no ML ranking complexity
- no alerts/monitoring yet


## 3) Feature spec (v1)

## F1. Seed graph neighborhood
`graph neighbors --seed <doi|paper_id> --direction in|out|both --limit N`

Output:
- outgoing citations (paper cites)
- incoming citations (paper cited by)
- confidence/source for each edge


## F2. Similar papers (structure-based)
`graph related --seed <doi|paper_id> --mode coupling|cocite --limit N`

- **coupling**: shared references (good for topical similarity)
- **cocite**: co-cited with seed's references (good for conceptual proximity)


## F3. Prior / Derivative work views
Simple wrappers on top of neighbors + year sorting:
- `graph prior --seed ...`   => incoming/outgoing filtered by older year
- `graph derivative --seed ...` => newer year follow-ups

(Implementation can start as flags in `graph neighbors`.)


## F4. Seed set exploration (collection-level)
`graph related-set --seeds <doi1,doi2,...> --mode coupling --limit N`

Aggregate overlap score across multiple seed papers.
This mirrors collection-based exploration behavior in tools like ResearchRabbit/Litmaps.


## 4) Ranking strategy (minimal)

For each candidate, compute:
- overlap count (shared refs or co-cite frequency)
- edge confidence bonus (high DOI match preferred)
- recency tie-break (year desc)

No ML ranker yet.


## 5) Data quality dependencies

Must-have for reliable recommendations:
- DOI normalization quality
- reference DOI extraction coverage
- edge resolution ratio

Track metrics:
- paper_count
- reference_count
- reference_with_doi_count
- edge_count
- avg_out_degree


## 6) Implementation order (short)

1. Keep current `graph ingest-doi`, `graph stats`, `graph neighbors`, `graph related`
2. Add `prior/derivative` filtering in service layer
3. Add `related-set` query in repository + service
4. Add CLI/MCP parity for new commands
5. Add tests with synthetic graph fixtures for each mode


## 7) Acceptance criteria

Given a seed DOI with at least K resolved edges:
- neighbors returns deterministic, non-empty results
- related (coupling/cocite) returns stable top-N with overlap score
- prior/derivative views correctly sorted by year
- CLI/MCP outputs match schema and each other


## 8) Why this is the right next step

This gives users the key Connected Papers / ResearchRabbit workflow value (seed -> explore -> refine) without overbuilding.
It maximizes functional utility and code quality while staying in local/private-first architecture.
