from __future__ import annotations

import xml.etree.ElementTree as ET

from papersearch.ingest.models import Document
from papersearch.ingest.xml_extractors import (
    extract_abstract,
    extract_figures,
    extract_metadata,
    extract_object_map,
    extract_references,
    extract_sections,
    extract_tables,
    extract_title,
)


def parse_elsevier_xml(xml_text: str, doi: str | None = None) -> Document:
    root = ET.fromstring(xml_text)
    object_map = extract_object_map(root)

    return Document(
        doc_id=f"doi:{doi}" if doi else "unknown",
        source="elsevier",
        title=extract_title(root),
        abstract=extract_abstract(root),
        sections=extract_sections(root),
        figures=extract_figures(root, object_map),
        tables=extract_tables(root),
        equations=[],
        references=extract_references(root),
        metadata=extract_metadata(root, fallback_doi=doi),
        provenance={"doi": doi, "api": "elsevier_fulltext"},
    )
