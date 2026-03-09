from __future__ import annotations

import json
import re
from pathlib import Path

from papersearch.ingest.pipeline import ingest_doi

DOIS = [
    "10.1016/j.fuel.2026.138904",
    "10.1016/j.energy.2017.07.132",
    "10.1016/j.egyai.2024.100341",
]


def safe_name(doi: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", doi)


def main() -> None:
    out_dir = Path("data/live_markdown")
    out_dir.mkdir(parents=True, exist_ok=True)

    summary = []
    for doi in DOIS:
        r = ingest_doi(doi, use_mock=False)
        s = safe_name(doi)
        (out_dir / f"{s}.md").write_text(r["markdown"], encoding="utf-8")
        (out_dir / f"{s}.json").write_text(json.dumps(r["normalized"], ensure_ascii=False, indent=2), encoding="utf-8")
        (out_dir / f"{s}.fetch.json").write_text(json.dumps(r["fetch"], ensure_ascii=False, indent=2), encoding="utf-8")
        summary.append(
            {
                "doi": doi,
                "view": r["fetch"].get("view"),
                "status": r["fetch"].get("status"),
                "quality": r["quality"],
                "sections": len(r["normalized"].get("sections", [])),
                "figures": len(r["normalized"].get("figures", [])),
                "tables": len(r["normalized"].get("tables", [])),
                "references": len(r["normalized"].get("references", [])),
            }
        )

    (out_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
