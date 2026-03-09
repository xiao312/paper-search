# Ingestion & Normalization Spec
## Semantic Scholar + Elsevier (XML -> JSON -> Markdown)

## 1. Goal
Build a reliable literature ingestion pipeline that converts heterogeneous paper sources into a **single semantic markdown format** for search/classification/RAG.

Target workflow:
1) Discovery via Semantic Scholar APIs (DOI-first)
2) Route eligible DOI (`10.1016/*`) to Elsevier full-text API
3) Parse XML -> normalized JSON
4) Render canonical Markdown

## 2. Data Source Roles

### Semantic Scholar API (Discovery Layer)
Use for:
- query-based discovery
- citation/reference graph traversal
- metadata enrichment

Expected outputs:
- DOI, title, abstract, year, venue, authors, references/citations IDs

### Elsevier Research Products APIs (Full-Text Layer)
Use for:
- eligible full-text XML retrieval (not PDF)

Important operational constraint (from field practice):
- access may depend on institutional/campus IP and subscription entitlement
- valid API key alone may not guarantee full-text retrieval outside entitled network

## 3. Source Priority and Fallback
Per paper, construct text in this order:
1. Elsevier XML full-text (best)
2. other structured full text (future extension)
3. abstract + metadata fallback

This keeps pipeline robust when full text is unavailable.

## 4. Canonical Internal Format

### 4.1 Normalized JSON document
```json
{
  "doc_id": "doi:10.1016/...",
  "source": "elsevier|semanticscholar|mixed",
  "title": "...",
  "abstract": "...",
  "sections": [
    {"heading": "Introduction", "paragraphs": ["..."]}
  ],
  "figures": [{"id": "fig1", "caption": "..."}],
  "tables": [{"id": "tbl1", "caption": "...", "markdown": "|...|"}],
  "equations": [{"id": "eq1", "latex": "..."}],
  "references": [{"key": "[1]", "text": "...", "doi": "..."}],
  "provenance": {
    "doi": "10.1016/...",
    "retrieved_at": "...",
    "api": "elsevier_fulltext"
  }
}
```

### 4.2 Markdown render target
```md
# Title

## Abstract
...

## 1 Introduction
...

## Figures
- Fig 1: ...

## Tables
| ... |

## Equations
$$ ... $$

## References
1. ...
```

## 5. XML Cleaning Rules (Minimal v1)
1. Preserve hierarchy: section -> subsection -> paragraph
2. Remove layout noise: page headers/footers, decorative artifacts
3. Keep figure/table captions linked to ids
4. Keep equations as LaTeX/MathML-converted text if possible
5. Normalize references with DOI extraction when present
6. Preserve citation anchors in text (`[ref:doi]` optional)

## 6. Quality Gates
For each ingested document, emit checks:
- `has_title`
- `has_abstract_or_intro`
- `section_count`
- `reference_count`
- `equation_count`
- parse warnings/errors

Reject or flag doc when quality below threshold.

## 7. API Reliability & Compliance
- Respect rate limits and retry budgets
- Cache raw responses by DOI + ETag/version where applicable
- Store provenance for auditability
- Keep usage aligned with publisher/API terms

## 8. Implementation Modules (proposed)
- `ingest/discovery_semanticscholar.py`
- `ingest/fetch_elsevier_xml.py`
- `ingest/parse_elsevier_xml.py`
- `ingest/normalize_json.py`
- `ingest/render_markdown.py`
- `ingest/quality_checks.py`

## 9. Milestones

### M1 (now)
- Semantic Scholar discovery
- DOI routing + mock Elsevier fetch interface
- canonical JSON + markdown renderer

### M2
- Real Elsevier XML parsing for major article patterns
- equations/tables/references stable extraction

### M3
- round-trip QA + retrieval chunk quality evaluation
- classifier integration using markdown-first input

## 10. Why this plan is careful but pragmatic
- Uses existing mature APIs instead of reinventing crawling
- Maximizes semantic signal for AI workflows
- Keeps architecture minimal and testable
- Handles entitlement/network constraints explicitly
