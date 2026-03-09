from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class Section:
    heading: str = ""
    paragraphs: list[str] = field(default_factory=list)


@dataclass
class Figure:
    id: str = ""
    label: str = ""
    caption: str = ""
    href: str = ""
    locator: str = ""
    asset_ref: str = ""
    asset_url: str = ""
    asset_local_path: str = ""
    asset_rel_path: str = ""
    asset_mime: str = ""
    asset_error: str = ""


@dataclass
class Table:
    id: str = ""
    label: str = ""
    caption: str = ""
    markdown: str = ""


@dataclass
class Equation:
    latex: str = ""


@dataclass
class Reference:
    key: str = ""
    text: str = ""
    doi: str | None = None


@dataclass
class Document:
    doc_id: str = "unknown"
    source: str = "unknown"
    title: str = ""
    abstract: str = ""
    sections: list[Section] = field(default_factory=list)
    figures: list[Figure] = field(default_factory=list)
    tables: list[Table] = field(default_factory=list)
    equations: list[Equation] = field(default_factory=list)
    references: list[Reference] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    provenance: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def as_document(doc: Document | dict[str, Any]) -> Document:
    if isinstance(doc, Document):
        return doc

    return Document(
        doc_id=doc.get("doc_id") or "unknown",
        source=doc.get("source") or "unknown",
        title=doc.get("title") or "",
        abstract=doc.get("abstract") or "",
        sections=[Section(**s) for s in (doc.get("sections") or [])],
        figures=[Figure(**f) for f in (doc.get("figures") or [])],
        tables=[Table(**t) for t in (doc.get("tables") or [])],
        equations=[Equation(**e) for e in (doc.get("equations") or [])],
        references=[Reference(**r) for r in (doc.get("references") or [])],
        metadata=doc.get("metadata") or {},
        provenance=doc.get("provenance") or {},
    )
