# Product Philosophy & Positioning (v1)

Last updated: 2026-03-09

## 1) Why this product exists
General web-search APIs optimize for broad internet retrieval.
Our product optimizes for **research literature sense-making**:
- reliable paper discovery,
- machine-usable normalization,
- and agent-driven, iterative exploration over citation/method/question graphs.

## 2) First-Principles Framing

### Core problem
Researchers/agents do not just need “answers”; they need:
1. source-grounded literature coverage,
2. structured representations of papers,
3. traceable exploration across many possible research paths.

### Irreducible requirements
- Must ingest from existing high-signal scholarly sources (API-first).
- Must preserve provenance and normalize into AI-friendly formats.
- Must support iterative exploration, not one-shot retrieval.

### Derived design choices
- Local/private-first phase (CLI + MCP + outbound notifications only).
- Schema/contract-first outputs for reproducibility.
- Graceful degradation when full text is unavailable.

## 3) Occam’s Razor (what we deliberately keep simple)
- No public inbound service in phase 1.
- No heavy orchestration platform before need is proven.
- One canonical normalized document model.
- Minimal adapters: CLI + MCP.
- Add complexity only when quality/coverage metrics justify it.

## 4) Positioning vs Exa-like products

Exa and similar products are excellent at **web-scale retrieval APIs**.
Our positioning is different:

### We are not
- A generic web search API competitor.
- A replacement for broad internet search engines.

### We are
- A **research-literature operating layer** for agents.
- A system focused on paper metadata/full-text normalization and graph-driven exploration.
- A workflow that turns raw papers into reusable, machine-readable research assets.

### Complementarity
We can still use Exa-like services as supporting signals (news/context/docs), while keeping scholarly sources as primary truth for literature tasks.

## 5) Nonlinear exploration philosophy (agent-native research)
Inspired by “massive, nonlinear exploration” ideas:
- treat research exploration as many parallel branches,
- do not require immediate convergence to one “master answer”,
- accumulate branch outputs as reusable evidence units.

In this repo context, that means:
- branchable search rounds,
- explicit provenance per branch,
- accumulation of findings even if branches are never merged,
- cross-branch inspiration through shared graph structures.

## 6) Graph-first worldview for literature sense-making
Citation graph is only one view. We should support multiple coexisting views:
- Citation / reference graph
- Method lineage graph (method A -> improved by B -> generalized by C)
- Research question graph (question decomposition and coverage)
- Dataset/benchmark graph
- Claim-evidence graph (claim nodes backed by sections/figures/tables)
- Institution/author collaboration graph (optional enrichment)

These graphs become the substrate for distributed, agentic exploration.

## 7) Product theses (current)
1. **Normalization before intelligence**: robust structured ingestion is higher leverage than early fancy reasoning.
2. **Graphs over linear summaries**: graphs preserve alternatives and evolution paths.
3. **Branch accumulation over forced merge**: keep useful branch outputs even when not globally reconciled.
4. **API-first over scraping-first**: lower legal/maintenance risk and higher reliability.
5. **Private-first by default**: practical adoption for sensitive research workflows.

## 8) Practical near-term implications
- Keep improving Elsevier/S2 ingestion quality and fallback behavior.
- Add additional sources incrementally (OpenAlex/Crossref/Unpaywall/CORE first).
- Add explicit branch IDs and branch-level metrics in iterative loops.
- Add graph materialization from normalized JSON/Markdown artifacts.

## 9) Adjacent products/services scanned (for market context)
- Exa Search API: https://exa.ai/docs/reference/search
- Tavily: https://docs.tavily.com/api-reference/endpoint/search
- Perplexity Sonar API: https://docs.perplexity.ai/docs/sonar/quickstart
- Brave Search API: https://api-dashboard.search.brave.com/app/documentation
- You.com developer search: https://documentation.you.com/get-started/quickstart
- SerpApi (incl. Google Scholar wrappers): https://serpapi.com/google-scholar-api
- Jina Reader / grounding tools: https://jina.ai/news/reader-lm-small-language-models-for-cleaning-and-converting-html-to-markdown

These are useful references and potential complements, not direct replacements for literature-centric ingestion + graph exploration.
