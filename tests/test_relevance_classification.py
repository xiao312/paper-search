import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from papersearch.app.repository import Repo
from papersearch.app.service import AppService


class TestRelevanceClassification(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        db = str(Path(self.tmp.name) / "test.db")
        self.svc = AppService(repo=Repo(db_path=db), notifier=None)

    def tearDown(self):
        self.tmp.cleanup()

    def test_relevance_classify_query_id(self):
        papers = {
            "code": 0,
            "items": [
                {
                    "title": "Paper A",
                    "doi": "10.1/a",
                    "abstract": "This work studies ammonia natural gas combustion kinetics with ML surrogates.",
                    "journal": "Fuel",
                    "publication_date": "2024-01-01",
                    "relevance_score": 0.9,
                    "sort_score": 0.9,
                },
                {
                    "title": "Paper B",
                    "doi": "10.1/b",
                    "abstract": "",
                    "journal": "Fuel",
                    "publication_date": "2024-01-01",
                    "relevance_score": 0.8,
                    "sort_score": 0.8,
                },
            ],
            "source_list": ["Bohrium"],
            "log_id": 1,
            "error": None,
        }
        with patch.object(self.svc, "bohrium_question_papers", return_value=papers), patch.object(
            self.svc,
            "llm_prompt",
            return_value={"ok": True, "response": '{"label":"highly_relevant","reason":"directly on topic"}', "stderr": ""},
        ):
            out = self.svc.relevance_classify_query_id(topic="ammonia combustion ML", query_id="6486", top_k=2, max_workers=1)

        self.assertEqual(out["counts"]["highly_relevant"], 1)
        self.assertEqual(out["counts"]["non_classifiable"], 1)


if __name__ == "__main__":
    unittest.main()
