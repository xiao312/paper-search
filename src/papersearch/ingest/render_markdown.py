from __future__ import annotations

import re

from papersearch.ingest.models import Document, as_document


def _render_figure_block(f) -> list[str]:
    out: list[str] = []
    fid = f.id or ""
    label = f.label or ""
    cap = f.caption or ""
    local = f.asset_rel_path or f.asset_local_path or ""
    href = f.href or ""
    locator = f.locator or ""
    prefix = label or fid or "Figure"

    out.append(f"### {prefix}")
    if cap:
        out.extend([cap, ""])

    if local:
        out.extend([f"![{prefix}]({local})", ""])
    elif href or locator:
        asset = href or locator
        out.extend([f"![{prefix}](elsevier://{asset})", ""])

    return out


def _figure_ref_patterns(label: str) -> list[str]:
    pats = []
    label = (label or "").strip()
    if not label:
        return pats
    pats.append(re.escape(label))
    m = re.search(r"(\d+)", label)
    if m:
        n = m.group(1)
        pats.append(rf"Fig\.\s*{n}")
        pats.append(rf"Figure\s*{n}")
    return pats


def render_markdown(doc: Document | dict) -> str:
    d = as_document(doc)
    lines: list[str] = []
    lines.append(f"# {d.title or 'Untitled'}")
    lines.append("")

    if d.abstract:
        lines.extend(["## Abstract", "", d.abstract, ""])

    inserted_fig_ids: set[str] = set()
    for i, sec in enumerate(d.sections, start=1):
        heading = sec.heading or f"Section {i}"
        lines.extend([f"## {i}. {heading}", ""])
        for p in sec.paragraphs:
            lines.extend([p, ""])
            for f in d.figures:
                fid = f.id or ""
                if fid and fid in inserted_fig_ids:
                    continue
                pats = _figure_ref_patterns(f.label or fid)
                if any(re.search(pt, p, flags=re.I) for pt in pats):
                    lines.extend(_render_figure_block(f))
                    if fid:
                        inserted_fig_ids.add(fid)

    remaining = [f for f in d.figures if (f.id or "") not in inserted_fig_ids]
    if remaining:
        lines.extend(["## Figures", ""])
        for f in remaining:
            lines.extend(_render_figure_block(f))
        lines.append("")

    if d.tables:
        lines.extend(["## Tables", ""])
        for t in d.tables:
            lines.append(f"### {t.label or t.id or 'Table'}")
            if t.caption:
                lines.extend([t.caption, ""])
            if t.markdown:
                lines.extend([t.markdown, ""])

    if d.equations:
        lines.extend(["## Equations", ""])
        for e in d.equations:
            if e.latex:
                lines.extend(["$$", e.latex, "$$", ""])

    if d.references:
        lines.extend(["## References", ""])
        for idx, r in enumerate(d.references, start=1):
            lines.append(f"{idx}. {r.text}")
        lines.append("")

    return "\n".join(lines).strip() + "\n"
