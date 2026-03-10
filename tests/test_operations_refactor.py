import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from papersearch.app.repository import Repo
from papersearch.app.service import AppService


class TestOperationsRefactor(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        db = str(Path(self.tmp.name) / "test.db")
        self.repo = Repo(db_path=db)
        self.svc = AppService(repo=self.repo, notifier=None)

    def tearDown(self):
        self.tmp.cleanup()

    def test_op_search(self):
        with patch.object(
            self.svc,
            "seed_candidates",
            return_value={"seeds": [{"paper_id": "doi:10.1/a"}], "seed_count": 1, "query_id": "q1", "bohrium_meta": {}, "crossref_topup": None, "fallback_attempts": []},
        ):
            out = self.svc.op_search("ammonia combustion")
        self.assertEqual(out["operation"], "search")
        self.assertEqual(out["new_paper_count"], 1)

    def test_op_classify_non_classifiable(self):
        out = self.svc.op_classify(topic="ammonia combustion", candidates=[{"doi": "10.1/a", "title": "A", "abstract": ""}], top_k=1)
        self.assertEqual(out["operation"], "relevance_classification")
        self.assertEqual(out["counts"]["non_classifiable"], 1)

    def test_op_grow(self):
        now = "2026-01-01T00:00:00Z"
        self.repo.upsert_api_paper(
            {
                "paper_id": "doi:10.1/a",
                "doi": "10.1/a",
                "openalex_id": None,
                "citation_count": 0,
                "title": "A",
                "year": 2020,
                "venue": "Fuel",
                "abstract": "",
                "source": "test",
                "updated_at": now,
            }
        )
        self.repo.upsert_api_paper(
            {
                "paper_id": "doi:10.1/b",
                "doi": "10.1/b",
                "openalex_id": None,
                "citation_count": 0,
                "title": "B",
                "year": 2021,
                "venue": "Fuel",
                "abstract": "",
                "source": "test",
                "updated_at": now,
            }
        )
        self.repo.upsert_api_paper(
            {
                "paper_id": "doi:10.1/c",
                "doi": "10.1/c",
                "openalex_id": None,
                "citation_count": 0,
                "title": "C",
                "year": 2022,
                "venue": "Fuel",
                "abstract": "",
                "source": "test",
                "updated_at": now,
            }
        )
        self.repo.replace_api_references("doi:10.1/a", [{"src_paper_id": "doi:10.1/a", "ref_order": 1, "doi": "10.1/b", "ref_openalex_id": None, "raw_text": ""}])
        self.repo.replace_api_references("doi:10.1/b", [{"src_paper_id": "doi:10.1/b", "ref_order": 1, "doi": "10.1/c", "ref_openalex_id": None, "raw_text": ""}])
        self.repo.resolve_edges_doi_match(now)

        out = self.svc.op_grow(["10.1/a"], levels=2)
        self.assertEqual(out["operation"], "grow")
        self.assertTrue(out["total_discovered_unique"] >= 2)


if __name__ == "__main__":
    unittest.main()
