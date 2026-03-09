from __future__ import annotations

import os
import re
import urllib.request
from pathlib import Path

from papersearch.ingest.http import get_with_retry
from papersearch.ingest.models import Figure


def _ext_from_url(url: str) -> str:
    m = re.search(r"\.([a-zA-Z0-9]+)(?:\?|$)", url)
    return m.group(1).lower() if m else "jpg"


def fetch_figure_assets(figures: list[Figure], out_dir: str, api_key: str | None = None) -> list[Figure]:
    if not figures:
        return figures

    Path(out_dir).mkdir(parents=True, exist_ok=True)
    api_key = api_key or os.getenv("ELSEVIER_API_KEY")

    for idx, fig in enumerate(figures, start=1):
        if not fig.asset_url:
            continue

        ext = _ext_from_url(fig.asset_url)
        label = fig.label or fig.id or f"fig_{idx}"
        safe = re.sub(r"[^A-Za-z0-9._-]+", "_", label).strip("_") or f"fig_{idx}"
        path = Path(out_dir) / f"{safe}.{ext}"

        headers = {"Accept": "*/*"}
        if api_key:
            headers["X-ELS-APIKey"] = api_key

        req = urllib.request.Request(fig.asset_url, headers=headers, method="GET")
        try:
            content, resp_headers = get_with_retry(req, timeout=30, retries=3)
            path.write_bytes(content)
            fig.asset_local_path = str(path)
            fig.asset_mime = resp_headers.get("Content-Type", "")
        except Exception as e:
            fig.asset_error = str(e)

    return figures
