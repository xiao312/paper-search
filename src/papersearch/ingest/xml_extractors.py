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


def _normalize_person_name(name: str) -> str:
    name = (name or "").strip()
    if not name:
        return ""
    if "," in name:
        last, first = [x.strip() for x in name.split(",", 1)]
        if first and last:
            return f"{first} {last}"
    return name


def extract_metadata(root: ET.Element, fallback_doi: str | None = None) -> dict:
    md: dict[str, object] = {
        "doi": "",
        "pii": "",
        "journal": "",
        "cover_date": "",
        "volume": "",
        "issue": "",
        "page_range": "",
        "article_number": "",
        "publisher": "",
        "authors": [],
        "affiliations": [],
    }

    creators: list[str] = []
    affs: list[str] = []

    for e in root.iter():
        t = strip_ns(e.tag)
        txt = iter_text(e)
        if not txt:
            continue

        if t == "doi" and not md["doi"]:
            md["doi"] = txt
        elif t == "identifier" and not md["doi"] and txt.lower().startswith("doi:"):
            md["doi"] = txt.split(":", 1)[1].strip()
        elif t == "pii" and not md["pii"]:
            md["pii"] = txt
        elif t == "publicationName" and not md["journal"]:
            md["journal"] = txt
        elif t == "coverDate" and not md["cover_date"]:
            md["cover_date"] = txt
        elif t == "volume" and not md["volume"]:
            md["volume"] = txt
        elif t in ("number", "issueIdentifier") and not md["issue"]:
            md["issue"] = txt
        elif t == "pageRange" and not md["page_range"]:
            md["page_range"] = txt
        elif t == "articleNumber" and not md["article_number"]:
            md["article_number"] = txt
        elif t == "publisher" and not md["publisher"]:
            md["publisher"] = txt
        elif t == "creator":
            creators.append(txt)
        elif t == "textfn":
            affs.append(txt)

    if not md["doi"] and fallback_doi:
        md["doi"] = fallback_doi

    if not creators:
        # fallback to structured author nodes
        for au in root.iter():
            if strip_ns(au.tag) != "author":
                continue
            gn = ""
            sn = ""
            for ch in au.iter():
                tt = strip_ns(ch.tag)
                if tt == "given-name" and not gn:
                    gn = iter_text(ch)
                elif tt == "surname" and not sn:
                    sn = iter_text(ch)
            full = " ".join(x for x in [gn, sn] if x).strip()
            if full:
                creators.append(full)

    md["authors"] = list(dict.fromkeys(_normalize_person_name(c) for c in creators if c.strip()))
    md["affiliations"] = list(dict.fromkeys(a.strip() for a in affs if a.strip()))
    return md


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
