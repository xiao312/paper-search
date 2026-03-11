# API Sources Inventory (Paper Search & Related Tasks)

Last updated: 2026-03-09

## 1) Purpose
Keep a durable record of candidate APIs/services for paper discovery, metadata enrichment, open-access/full-text retrieval, citation graph expansion, and related research tooling.

This is an inventory document, not an implementation commitment.

## 2) Current Phase-1 Baseline (Implemented)

| Service | Role in pipeline | Access model | Status in repo | Notes |
|---|---|---|---|---|
| Semantic Scholar API | discovery + metadata | free API key, rate-limited | Implemented (`discovery_semanticscholar.py`) | Live use currently constrained by key/rate (429 without approved key). |
| Elsevier Article Retrieval API | full-text XML for eligible DOI | API key + entitlement/IP constraints | Implemented (`fetch_elsevier_xml.py`, XML parser pipeline) | View fallback implemented: `FULL -> ENTITLED -> META_ABS_REF -> META_ABS -> META`. |

Related implemented support:
- quality checks (`quality_checks.py`)
- normalized schema + markdown render
- figure asset download when URLs available

## 3) Candidate Free / Free-tier APIs (Tracked)

### 3.1 Discovery & Metadata

| Service | Primary use | Docs | Typical limits / caveats | Fit |
|---|---|---|---|---|
| OpenAlex | broad scholarly search; works/authors/venues/institutions; citation links | https://docs.openalex.org/ | free, polite usage expected | High |
| Crossref REST API | DOI metadata backbone | https://www.crossref.org/documentation/retrieve-metadata/rest-api/ | no key required for many use cases; add mailto/polite client style | High |
| DataCite REST API | DOI metadata for datasets/software/research objects | https://support.datacite.org/reference/introduction | free API access | Medium-High |
| Semantic Scholar API | ranked discovery + graph metadata | https://www.semanticscholar.org/product/api | key strongly recommended; rate limits | High (already integrated) |
| OpenAIRE Graph API | European research graph metadata | https://api.openaire.eu/graph/swagger-ui/index.html?urls.primaryName=OpenAIRE+Graph+API+V2 | API complexity higher | Medium |

### 3.2 Open Access / Full Text Retrieval Helpers

| Service | Primary use | Docs | Typical limits / caveats | Fit |
|---|---|---|---|---|
| Unpaywall | OA location resolver from DOI | https://support.unpaywall.org/support/solutions/articles/44001977396-how-do-i-use-the-title-search-api- | best used as resolver, not full metadata source | High |
| CORE API | OA full text + metadata | https://core.ac.uk/services/api | key/quotas may apply by plan | High |
| Europe PMC REST | biomedical paper discovery + links/full text where available | https://europepmc.org/RestfulWebService ; https://www.europepmc.org/developers | bio/medical domain focus | High (for bio scope) |
| arXiv API | preprint metadata/feed access | https://info.arxiv.org/help/api/index.html | preprint-only corpus | Medium-High |
| Zenodo API | repository records/files | https://developers.zenodo.org/ | repository-specific content | Medium |
| Elsevier APIs | publisher XML full text for eligible DOI | https://dev.elsevier.com/ | entitlement/network constraints | High (already integrated) |

### 3.3 Citation Graph & Bibliometrics

| Service | Primary use | Docs | Typical limits / caveats | Fit |
|---|---|---|---|---|
| OpenCitations | open citation links + bibliographic graph | https://opencitations.net/api/v1 ; https://api.opencitations.net/ | open citation coverage varies by source | Medium-High |

### 3.4 Entity Enrichment (People / Institutions / Journals)

| Service | Primary use | Docs | Typical limits / caveats | Fit |
|---|---|---|---|---|
| ORCID Public API | author identity/profile enrichment | https://info.orcid.org/documentation/features/public-api/ | public data only via public API | Medium |
| ROR API | institution normalization | https://ror.org/tags/api/ | organization registry only | Medium |
| DOAJ API | OA journal indexing/filtering signal | https://doaj.org/docs/api/ | journal-level signal, not full article graph | Medium |

### 3.5 Domain-specific (Biomedical)

| Service | Primary use | Docs | Typical limits / caveats | Fit |
|---|---|---|---|---|
| NCBI E-utilities (PubMed/PMC) | biomedical retrieval + metadata | https://www.ncbi.nlm.nih.gov/home/develop/api/ ; https://eutils.ncbi.nlm.nih.gov/entrez/query/static/eutils_help.html | domain-specific; usage policies apply | Medium-High (bio workflows) |

### 3.6 General Web Search for Supporting Tasks

| Service | Primary use | Docs | Typical limits / caveats | Fit |
|---|---|---|---|---|
| Exa Search API | web-scale supplementary retrieval, current events/docs | https://exa.ai/docs | not scholarly-only; separate relevance filtering needed | Medium (supporting tool) |

## 4) Suggested Integration Priority (If Expanded)
1. OpenAlex
2. Crossref
3. Unpaywall
4. CORE
5. OpenCitations
6. Europe PMC / NCBI E-utilities (when bio domain is needed)
7. DataCite / ORCID / ROR / DOAJ as enrichment layers

Rationale: maximize coverage and robustness with low coupling before adding niche/domain-specific sources.

## 5) Minimal Source-Orchestration Strategy (Reference)
Per DOI/query candidate:
1. Discovery: Semantic Scholar and/or OpenAlex
2. DOI metadata normalization: Crossref/DataCite
3. OA resolver: Unpaywall
4. Full text path:
   - Elsevier XML for `10.1016/*` when available
   - CORE/EuropePMC/arXiv/Zenodo fallback where applicable
5. Citation expansion: OpenCitations (+ S2/OpenAlex links)
6. Entity normalization: ORCID/ROR (optional)

## 6) Compliance & Security Notes
- Never commit API keys/tokens in repository docs or code.
- Respect each provider’s terms, rate limits, and attribution requirements.
- Cache responses and store provenance fields for auditability.
- Keep graceful degradation: metadata/abstract fallback when full text unavailable.

## 7) Related Project Docs
- `docs/specs/elsevier-semanticscholar-ingestion-spec.md`
- `docs/specs/search-loop-phases-api-first.md`
- `docs/architecture/careful-plan-roadmap.md`
