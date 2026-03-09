from __future__ import annotations

from papersearch.ingest.models import Document, as_document


def quality_report(doc: Document | dict) -> dict:
    d = as_document(doc)
    section_count = len(d.sections)
    ref_count = len(d.references)
    eq_count = len(d.equations)
    has_title = bool(d.title.strip())
    has_abstract_or_intro = bool(d.abstract.strip()) or section_count > 0

    warnings = []
    if not has_title:
        warnings.append("missing_title")
    if not has_abstract_or_intro:
        warnings.append("missing_abstract_or_intro")

    return {
        "has_title": has_title,
        "has_abstract_or_intro": has_abstract_or_intro,
        "section_count": section_count,
        "reference_count": ref_count,
        "equation_count": eq_count,
        "warnings": warnings,
        "ok": has_title and has_abstract_or_intro,
    }
