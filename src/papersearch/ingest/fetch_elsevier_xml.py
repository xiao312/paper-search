from __future__ import annotations

import os
import urllib.parse
import urllib.request

from papersearch.ingest.errors import ProviderError
from papersearch.ingest.http import get_with_retry


class ElsevierFullTextClient:
    BASE = "https://api.elsevier.com/content/article/doi"

    def __init__(self, api_key: str | None = None, inst_token: str | None = None, timeout: int = 20):
        self.api_key = api_key or os.getenv("ELSEVIER_API_KEY")
        self.inst_token = inst_token or os.getenv("ELSEVIER_INST_TOKEN")
        self.timeout = timeout

    def fetch_xml_by_doi(self, doi: str, use_mock: bool = False) -> tuple[str | None, dict]:
        if use_mock:
            return self._mock_xml(doi), {"status": 200, "source": "elsevier-mock", "view": "FULL"}

        if not self.api_key:
            return None, {"status": 0, "error": "ELSEVIER_API_KEY not set"}

        view_order = ["FULL", "ENTITLED", "META_ABS_REF", "META_ABS", "META"]
        attempts = []
        best_xml = None
        best_meta = None
        best_score = -1

        for view in view_order:
            xml, meta = self._fetch_once(doi=doi, view=view)
            attempts.append({"view": view, **meta})

            if xml is None:
                continue

            score = self._richness_score(xml)
            # prefer richer payload; tie-break by longer payload
            if score > best_score or (score == best_score and best_xml is not None and len(xml) > len(best_xml)) or best_xml is None:
                best_xml = xml
                best_meta = {"status": meta.get("status", 0), "source": "elsevier", "view": view, "richness": score}
                best_score = score

            # early stop: rich enough structured full text
            if score >= 20 and view in ("FULL", "ENTITLED"):
                break

        if best_xml is not None:
            best_meta["attempts"] = attempts  # type: ignore[index]
            return best_xml, best_meta  # type: ignore[arg-type]

        return None, {"status": 0, "source": "elsevier", "error": "all views failed", "attempts": attempts}

    def _fetch_once(self, doi: str, view: str) -> tuple[str | None, dict]:
        q = urllib.parse.urlencode({"view": view, "httpAccept": "text/xml"})
        url = f"{self.BASE}/{urllib.parse.quote(doi, safe='')}?{q}"
        headers = {
            "X-ELS-APIKey": self.api_key,
            "Accept": "text/xml",
        }
        if self.inst_token:
            headers["X-ELS-Insttoken"] = self.inst_token

        req = urllib.request.Request(url, headers=headers, method="GET")
        try:
            data, _headers = get_with_retry(req, timeout=self.timeout, retries=3)
            return data.decode("utf-8", errors="replace"), {"status": 200}
        except ProviderError as e:
            msg = str(e)
            status = 0
            if msg.startswith("HTTP "):
                try:
                    status = int(msg.split()[1].rstrip(":"))
                except Exception:
                    status = 0
            return None, {"status": status, "error": msg[:1000]}

    @staticmethod
    def _richness_score(xml: str) -> int:
        markers = ["<ce:section", "<section", "<ce:para", "<para", "<ce:simple-para", "<simple-para", "<ce:figure", "<figure", "<ce:table", "<table", "<bib-reference", "<reference"]
        return sum(xml.count(m) for m in markers)

    @staticmethod
    def _mock_xml(doi: str) -> str:
        return f"""<?xml version='1.0' encoding='UTF-8'?>
<root>
  <article-title>Mock Elsevier Article for {doi}</article-title>
  <abstract><para>This is a mock abstract for {doi}.</para></abstract>
  <section>
    <title>Introduction</title>
    <para>Intro paragraph one.</para>
    <para>Intro paragraph two.</para>
  </section>
  <section>
    <title>Methods</title>
    <para>Method details.</para>
  </section>
  <figure id='fig1'><caption>Mock figure caption</caption></figure>
  <ref-list>
    <ref id='r1'>Reference one text. doi:10.1000/xyz123</ref>
  </ref-list>
</root>
"""
