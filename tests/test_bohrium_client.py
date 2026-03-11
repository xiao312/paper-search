import unittest
from unittest.mock import patch

from papersearch.ingest.discovery_bohrium import BohriumSigmaSearchClient


class TestBohriumSigmaSearchClient(unittest.TestCase):
    def test_create_session_parse(self):
        sample = {"code": 0, "data": {"uuid": "u-1", "title": "q", "share": False}}
        with patch("papersearch.ingest.discovery_bohrium.get_json_with_retry", return_value=sample):
            out = BohriumSigmaSearchClient(access_key="k").create_session("ammonia combustion")
        self.assertEqual(out["uuid"], "u-1")

    def test_session_detail_parse(self):
        sample = {
            "code": 0,
            "data": {
                "uuid": "u-1",
                "title": "q",
                "status": "done",
                "model": "auto",
                "discipline": "All",
                "questions": [{"id": 6486, "query": "q", "status": "done", "lastAnswerID": 1}],
            },
        }
        with patch("papersearch.ingest.discovery_bohrium.get_json_with_retry", return_value=sample):
            out = BohriumSigmaSearchClient(access_key="k").get_session_detail("u-1")
        self.assertEqual(out["query_id"], "6486")

    def test_question_papers_parse(self):
        sample = {
            "code": 200,
            "data": {
                "list": [
                    {
                        "sequenceId": 1,
                        "author": ["A", "B"],
                        "link": "https://example.org",
                        "source": "Elsevier",
                        "sourceZh": "爱思唯尔",
                        "abstract": "abc",
                        "abstractZh": "中文",
                        "title": "Title",
                        "titleZh": "标题",
                        "doi": "10.1/X",
                        "bohriumId": "bh-1",
                        "publicationId": 1,
                        "publicationCover": "",
                        "publicationDate": "2026-01-01",
                        "journal": "Fuel",
                        "arxiv": "",
                        "aiSummarize": "sum",
                        "openAccess": "Y",
                        "pdfFlag": True,
                        "pieces": "",
                        "sortScore": 0.9,
                        "relevanceScore": 0.8,
                        "publicationScore": 1,
                        "impactFactor": 2.1,
                        "impactFactorScore": 0.5,
                        "citationNums": 10,
                        "fullText": "",
                        "figures": [],
                    }
                ],
                "sourceList": ["Elsevier"],
                "logId": 123,
            },
        }

        with patch("papersearch.ingest.discovery_bohrium.get_json_with_retry", return_value=sample):
            out = BohriumSigmaSearchClient(access_key="k").question_papers("6486")

        self.assertEqual(out["code"], 200)
        self.assertEqual(out["query_id"], "6486")
        self.assertEqual(len(out["items"]), 1)
        self.assertEqual(out["items"][0]["doi"], "10.1/x")


if __name__ == "__main__":
    unittest.main()
