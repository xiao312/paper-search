from __future__ import annotations

import re
import xml.etree.ElementTree as ET

from papersearch.ingest.models import Figure, Reference, Section, Table


XLINK_HREF = "{http://www.w3.org/1999/xlink}href"


def strip_ns(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def iter_text(elem: ET.Element) -> str:
    txt = " ".join(t.strip() for t in elem.itertext() if t and t.strip())
    txt = re.sub(r"\s+", " ", txt).strip()
    txt = re.sub(r"\(\s+", "(", txt)
    txt = re.sub(r"\s+\)", ")", txt)
    txt = re.sub(r"\s+([,.;:])", r"\1", txt)
    return txt


def extract_title(root: ET.Element) -> str:
    for e in root.iter():
        if strip_ns(e.tag) == "title":
            txt = iter_text(e)
            if txt:
                return txt
    return ""


def extract_abstract(root: ET.Element) -> str:
    for e in root.iter():
        if strip_ns(e.tag) == "abstract":
            txt = iter_text(e)
            if txt:
                return txt

    for e in root.iter():
        if strip_ns(e.tag) == "description":
            txt = iter_text(e)
            if txt:
                return txt
    return ""


def extract_sections(root: ET.Element) -> list[Section]:
    sections: list[Section] = []
    for sec in root.iter():
        if strip_ns(sec.tag) not in ("section", "sec"):
            continue
        heading = ""
        paras: list[str] = []
        for ch in list(sec):
            t = strip_ns(ch.tag)
            if t in ("title", "section-title") and not heading:
                heading = iter_text(ch)
            elif t in ("para", "p", "simple-para"):
                txt = iter_text(ch)
                if txt:
                    paras.append(txt)
        if paras:
            sections.append(Section(heading=heading, paragraphs=paras))

    if not sections:
        for e in root.iter():
            if strip_ns(e.tag) == "rawtext":
                txt = iter_text(e)
                if txt:
                    sections.append(Section(heading="Body", paragraphs=[txt]))
                    break
    return sections


def extract_object_map(root: ET.Element) -> dict[str, str]:
    out: dict[str, str] = {}
    for obj in root.iter():
        if strip_ns(obj.tag) != "object":
            continue
        ref = obj.attrib.get("ref", "")
        url = iter_text(obj)
        if ref and url.startswith("http"):
            out[ref] = url
    return out


def extract_figures(root: ET.Element, object_map: dict[str, str]) -> list[Figure]:
    figures: list[Figure] = []
    for fig in root.iter():
        if strip_ns(fig.tag) != "figure":
            continue

        cap = ""
        label = ""
        href = ""
        locator = ""
        for ch in fig.iter():
            t = strip_ns(ch.tag)
            if t == "label" and not label:
                label = iter_text(ch)
            elif t == "caption" and not cap:
                cap = iter_text(ch)
            elif t == "link":
                href = ch.attrib.get(XLINK_HREF, "") or href
                locator = ch.attrib.get("locator", "") or locator

        ref_key = locator or (href.split("/")[-1] if href else "")
        if not label and not re.match(r"^gr\d+$", ref_key or ""):
            continue

        figures.append(
            Figure(
                id=fig.attrib.get("id", ""),
                label=label,
                caption=cap,
                href=href,
                locator=locator,
                asset_ref=ref_key,
                asset_url=object_map.get(ref_key, ""),
            )
        )
    return figures


def _table_to_markdown(tbl: ET.Element) -> str:
    rows = []
    for r in tbl.iter():
        if strip_ns(r.tag) != "row":
            continue
        cells = [iter_text(c) for c in r if strip_ns(c.tag) == "entry"]
        if cells:
            rows.append(cells)
    if not rows:
        return ""

    max_cols = max(len(r) for r in rows)
    rows = [r + [""] * (max_cols - len(r)) for r in rows]
    head, body = rows[0], rows[1:]
    lines = [
        "| " + " | ".join(head) + " |",
        "| " + " | ".join(["---"] * max_cols) + " |",
    ]
    lines.extend("| " + " | ".join(r) + " |" for r in body)
    return "\n".join(lines)


def extract_tables(root: ET.Element) -> list[Table]:
    tables: list[Table] = []
    for tbl in root.iter():
        if strip_ns(tbl.tag) != "table":
            continue
        cap = ""
        label = ""
        for ch in tbl.iter():
            t = strip_ns(ch.tag)
            if t == "label" and not label:
                label = iter_text(ch)
            elif t == "caption" and not cap:
                cap = iter_text(ch)
        tables.append(Table(id=tbl.attrib.get("id", ""), label=label, caption=cap, markdown=_table_to_markdown(tbl)))
    return tables


def _clean_ref_text(text: str) -> str:
    text = re.sub(r"^\[\d+\]\s*", "", text).strip()
    text = re.sub(r"\s+", " ", text)
    doi_m = re.search(r"10\.\d{4,9}/[-._;()/:A-Z0-9]+", text, flags=re.I)
    if doi_m:
        suffix = text[doi_m.end():].strip(" .;,")
        if re.search(r"\b[A-Z]\.\s*(?:[A-Z]\.\s*)?[A-Za-z][A-Za-z-]+", suffix):
            text = suffix
    m = re.search(r"\b[A-Z]\.\s*(?:[A-Z]\.\s*)?[A-Za-z][A-Za-z-]+", text)
    if m and m.start() > 30:
        text = text[m.start():]
    text = re.sub(r"\s+([,.;:])", r"\1", text)
    return re.sub(r"\s+", " ", text).strip()


def extract_references(root: ET.Element) -> list[Reference]:
    refs: list[Reference] = []
    for tag in ("bib-reference", "reference", "ref"):
        bucket: list[Reference] = []
        for ref in root.iter():
            if strip_ns(ref.tag) != tag:
                continue
            text = _clean_ref_text(iter_text(ref))
            if not text:
                continue
            doi_m = re.search(r"10\.\d{4,9}/[-._;()/:A-Z0-9]+", text, flags=re.I)
            bucket.append(Reference(key=ref.attrib.get("id", "") or ref.attrib.get("refid", ""), text=text, doi=doi_m.group(0) if doi_m else None))
        if bucket:
            refs = bucket
            break

    seen: dict[str, Reference] = {}
    for r in refs:
        k = re.sub(r"[^a-z0-9 ]", "", re.sub(r"\s+", " ", r.text.lower()).strip())
        if not k:
            continue
        prev = seen.get(k)
        if prev is None or len(r.text) > len(prev.text):
            seen[k] = r
    return list(seen.values())
