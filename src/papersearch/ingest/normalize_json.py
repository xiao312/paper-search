from __future__ import annotations

from papersearch.ingest.models import Document, as_document


def normalize_document(doc: Document | dict) -> Document:
    out = as_document(doc)
    out.title = out.title.strip()
    out.abstract = out.abstract.strip()

    dedup_sections = []
    seen = set()
    for sec in out.sections:
        sec.heading = (sec.heading or "").strip()
        sec.paragraphs = [p.strip() for p in sec.paragraphs if p and p.strip()]
        if not sec.paragraphs:
            continue
        key = (sec.heading.lower(), sec.paragraphs[0][:200].lower())
        if key in seen:
            continue
        seen.add(key)
        dedup_sections.append(sec)

    out.sections = dedup_sections
    return out
