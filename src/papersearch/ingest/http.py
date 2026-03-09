from __future__ import annotations

import json
import time
import urllib.error
import urllib.request

from papersearch.ingest.errors import ProviderBadInput, ProviderError, ProviderRateLimited, ProviderUnauthorized


TRANSIENT_CODES = {429, 500, 502, 503, 504}


def get_with_retry(req: urllib.request.Request, timeout: int = 20, retries: int = 3, backoff_seconds: float = 0.8) -> tuple[bytes, dict]:
    last_exc: Exception | None = None
    for i in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read(), dict(resp.headers)
        except urllib.error.HTTPError as e:  # type: ignore[attr-defined]
            body = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else ""
            if e.code in TRANSIENT_CODES and i < retries - 1:
                time.sleep(backoff_seconds * (2**i))
                continue
            if e.code == 429:
                raise ProviderRateLimited(f"HTTP 429: {body[:400]}")
            if e.code in (401, 403):
                raise ProviderUnauthorized(f"HTTP {e.code}: {body[:400]}")
            if e.code == 400:
                raise ProviderBadInput(f"HTTP 400: {body[:400]}")
            raise ProviderError(f"HTTP {e.code}: {body[:400]}")
        except Exception as e:
            last_exc = e
            if i < retries - 1:
                time.sleep(backoff_seconds * (2**i))
                continue
            break
    raise ProviderError(str(last_exc) if last_exc else "request failed")


def get_json_with_retry(req: urllib.request.Request, timeout: int = 20, retries: int = 3) -> dict:
    data, _ = get_with_retry(req, timeout=timeout, retries=retries)
    return json.loads(data.decode("utf-8"))
