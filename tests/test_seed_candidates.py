import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from papersearch.app.repository import Repo
from papersearch.app.service import AppService


class TestSeedCandidates(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        db = str(Path(self.tmp.name) / "test.db")
        self.svc = AppService(repo=Repo(db_path=db), notifier=None)

    def tearDown(self):
        self.tmp.cleanup()

    def test_seed_candidates(self):
        with patch.object(
            self.svc,
            "llm_prompt",
            return_value={"ok": True, "response": '{"expanded_query":"ammonia natural gas combustion kinetics"}', "stderr": ""},
        ), patch.object(
            self.svc,
            "bohrium_question_papers",
            return_value={
                "code": 0,
                "items": [
                    {"doi": "10.1/A", "arxiv": "", "title": "A", "journal": "Fuel", "relevance_score": 0.9, "sort_score": 0.9},
                    {"doi": "", "arxiv": "2501.12345", "title": "B", "journal": "arXiv", "relevance_score": 0.8, "sort_score": 0.8},
                ],
                "source_list": ["Comprehensive"],
                "log_id": 1,
                "error": None,
            },
        ):
            out = self.svc.seed_candidates_from_query_id(query="ammonia combustion", query_id="6486", top_k=2)

        self.assertEqual(out["seed_count"], 2)
        self.assertEqual(out["expanded_query"], "ammonia natural gas combustion kinetics")
        self.assertEqual(out["seeds"][0]["paper_id"], "doi:10.1/a")

    def test_seed_candidates_auto(self):
        with patch.object(self.svc, "bohrium_create_session", return_value={"uuid": "u1", "code": 0, "error": None}), patch.object(
            self.svc,
            "bohrium_session_detail",
            return_value={"query_id": "6486", "questions": [{"status": "done"}], "code": 0, "error": None},
        ), patch.object(
            self.svc,
            "seed_candidates_from_query_id",
            return_value={"seed_count": 1, "seeds": [{"paper_id": "doi:10.1/a"}], "bohrium_meta": {"code": 0}, "expanded_query": "ammonia combustion"},
        ), patch.object(
            self.svc,
            "bohrium_question_papers",
            return_value={"items": [], "error": None},
        ):
            out = self.svc.seed_candidates("ammonia combustion", top_k=1, wait_seconds=0)
        self.assertEqual(out["seed_count"], 1)

    def test_seed_candidates_includes_mentioned_paper_from_local_db(self):
        self.svc.repo.upsert_api_paper(
            {
                "paper_id": "doi:10.1016/j.fuel.2026.138904",
                "doi": "10.1016/j.fuel.2026.138904",
                "openalex_id": None,
                "citation_count": 0,
                "title": "Enhancing deep learning of ammonia/natural gas combustion kinetics via physics-aware data augmentation and scale separation",
                "year": 2026,
                "venue": "Fuel",
                "abstract": "",
                "source": "test",
                "updated_at": "2026-01-01T00:00:00Z",
            }
        )

        with patch.object(
            self.svc,
            "llm_prompt",
            return_value={"ok": True, "response": '{"expanded_query":"similar methods"}', "stderr": ""},
        ), patch.object(
            self.svc,
            "bohrium_question_papers",
            return_value={"code": 0, "items": [], "source_list": ["Bohrium"], "log_id": 1, "error": None},
        ):
            out = self.svc.seed_candidates_from_query_id(
                query="search similar methods as 'Enhancing deep learning of ammonia/natural gas combustion kinetics via physics-aware data augmentation and scale separation'",
                query_id="6486",
                top_k=5,
            )

        self.assertEqual(out["seed_count"], 1)
        self.assertEqual(out["seeds"][0]["paper_id"], "doi:10.1016/j.fuel.2026.138904")

    def test_seed_candidates_auto_fallback_to_min_count(self):
        def _create_session(**kwargs):
            q = kwargs.get("query")
            if q == "q-main":
                return {"uuid": "u-main", "code": 0, "error": None}
            return {"uuid": "u-fallback", "code": 0, "error": None}

        def _session_detail(uuid, access_key=None):
            if uuid == "u-main":
                return {"query_id": "q1", "questions": [{"status": "done"}], "code": 0, "error": None}
            return {"query_id": "q2", "questions": [{"status": "done"}], "code": 0, "error": None}

        def _question_papers(query_id, sort="RelevanceScore", access_key=None):
            if query_id == "q2":
                return {
                    "items": [
                        {"doi": "10.2/a", "title": "A"},
                        {"doi": "10.2/b", "title": "B"},
                        {"doi": "10.2/c", "title": "C"},
                        {"doi": "10.2/d", "title": "D"},
                    ],
                    "error": None,
                }
            return {"items": [], "error": None}

        with patch.object(self.svc, "bohrium_create_session", side_effect=_create_session), patch.object(
            self.svc,
            "bohrium_session_detail",
            side_effect=_session_detail,
        ), patch.object(
            self.svc,
            "seed_candidates_from_query_id",
            return_value={"seed_count": 1, "seeds": [{"paper_id": "doi:10.1/a", "doi": "10.1/a"}], "expanded_query": "q-fallback", "bohrium_meta": {"code": 0}},
        ), patch.object(
            self.svc,
            "bohrium_question_papers",
            side_effect=_question_papers,
        ):
            out = self.svc.seed_candidates("q-main", top_k=5, wait_seconds=1, min_seed_count=5)

        self.assertGreaterEqual(out["seed_count"], 5)

    def test_seed_candidates_auto_crossref_topup(self):
        with patch.object(self.svc, "bohrium_create_session", return_value={"uuid": "u1", "code": 0, "error": None}), patch.object(
            self.svc,
            "bohrium_session_detail",
            return_value={"query_id": "q1", "questions": [{"status": "done"}], "code": 0, "error": None},
        ), patch.object(
            self.svc,
            "seed_candidates_from_query_id",
            return_value={"seed_count": 1, "seeds": [{"paper_id": "doi:10.1/a", "doi": "10.1/a"}], "expanded_query": "ammonia", "bohrium_meta": {"code": 0}},
        ), patch.object(
            self.svc,
            "bohrium_question_papers",
            return_value={"items": [], "error": None},
        ), patch.object(
            self.svc,
            "_topup_seeds_from_crossref",
            return_value=(
                [
                    {"paper_id": "doi:10.2/a", "doi": "10.2/a"},
                    {"paper_id": "doi:10.2/b", "doi": "10.2/b"},
                    {"paper_id": "doi:10.2/c", "doi": "10.2/c"},
                    {"paper_id": "doi:10.2/d", "doi": "10.2/d"},
                ],
                {"error": None},
            ),
        ):
            out = self.svc.seed_candidates("q-main", top_k=5, wait_seconds=1, min_seed_count=5, crossref_rows=30)

        self.assertGreaterEqual(out["seed_count"], 5)
        self.assertIn("crossref_topup", out)


if __name__ == "__main__":
    unittest.main()
