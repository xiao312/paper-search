from __future__ import annotations

import os
import re

from papersearch.ingest.discovery_semanticscholar import SemanticScholarClient
from papersearch.ingest.fetch_elsevier_xml import ElsevierFullTextClient
from papersearch.ingest.figure_assets import fetch_figure_assets
from papersearch.ingest.models import Document
from papersearch.ingest.normalize_json import normalize_document
from papersearch.ingest.parse_elsevier_xml import parse_elsevier_xml
from papersearch.ingest.quality_checks import quality_report
from papersearch.ingest.render_markdown import render_markdown


def discover_candidates(query: str, limit: int = 10, use_mock: bool = False) -> list[dict]:
    s2 = SemanticScholarClient()
    return s2.search(query=query, limit=limit, use_mock=use_mock)


def ingest_doi(
    doi: str,
    title: str = "",
    abstract: str = "",
    use_mock: bool = False,
    fetch_assets: bool = True,
    assets_root: str = "data/live_markdown/assets",
) -> dict:
    doi = (doi or "").strip()
    elsevier_eligible = doi.lower().startswith("10.1016/")

    raw_doc = Document(
        doc_id=f"doi:{doi}" if doi else "unknown",
        source="metadata-fallback",
        title=title or doi or "Untitled",
        abstract=abstract or "",
        provenance={"doi": doi, "api": "fallback"},
    )

    fetch_meta = {"status": None, "source": "fallback"}
    if elsevier_eligible:
        client = ElsevierFullTextClient()
        xml_text, fetch_meta = client.fetch_xml_by_doi(doi, use_mock=use_mock)
        if xml_text:
            raw_doc = parse_elsevier_xml(xml_text, doi=doi)

    normalized = normalize_document(raw_doc)

    asset_meta = {"enabled": False, "downloaded": 0}
    if fetch_assets and normalized.figures:
        safe = re.sub(r"[^A-Za-z0-9._-]+", "_", doi or "unknown")
        fig_dir = os.path.join(assets_root, safe)
        markdown_base_dir = os.path.dirname(assets_root) or "."

        before = sum(1 for f in normalized.figures if f.asset_local_path)
        normalized.figures = fetch_figure_assets(normalized.figures, out_dir=fig_dir)
        for f in normalized.figures:
            if f.asset_local_path:
                f.asset_rel_path = os.path.relpath(f.asset_local_path, start=markdown_base_dir)
        after = sum(1 for f in normalized.figures if f.asset_local_path)
        asset_meta = {"enabled": True, "downloaded": max(0, after - before), "dir": fig_dir, "markdown_base": markdown_base_dir}

    markdown = render_markdown(normalized)
    quality = quality_report(normalized)

    return {
        "doi": doi,
        "elsevier_eligible": elsevier_eligible,
        "fetch": fetch_meta,
        "assets": asset_meta,
        "normalized": normalized.to_dict(),
        "markdown": markdown,
        "quality": quality,
    }
