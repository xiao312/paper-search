from __future__ import annotations

import json
import os

from papersearch.ingest.pipeline import discover_candidates, ingest_doi


def main():
    query = os.getenv("LIVE_QUERY", "retrieval augmented generation")
    print("[LIVE] Semantic Scholar query:", query)
    try:
        items = discover_candidates(query, limit=3, use_mock=False)
        print(json.dumps({"count": len(items), "items": items[:2]}, ensure_ascii=False, indent=2)[:2000])
    except Exception as e:
        print("[LIVE] Semantic Scholar failed:", e)

    test_doi = os.getenv("LIVE_ELSEVIER_DOI", "10.1016/j.artint.2010.02.002")
    print("[LIVE] Elsevier DOI:", test_doi)
    out = ingest_doi(test_doi, use_mock=False)
    print(json.dumps({
        "eligible": out["elsevier_eligible"],
        "fetch": out["fetch"],
        "quality": out["quality"],
        "title": out["normalized"].get("title"),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
