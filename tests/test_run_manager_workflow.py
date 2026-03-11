import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from papersearch.app.repository import Repo
from papersearch.app.run_manager import RunManager
from papersearch.app.service import AppService


class TestRunManagerWorkflow(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        base = Path(self.tmp.name)
        self.repo = Repo(db_path=str(base / "app.db"))
        self.rm = RunManager(base_dir=str(base / "runs"))
        self.svc = AppService(repo=self.repo, notifier=None, run_manager=self.rm)

    def tearDown(self):
        self.tmp.cleanup()

    def test_run_end_to_end(self):
        run = self.svc.run_start("ammonia combustion kinetics")
        run_id = run["run_id"]

        with patch.object(
            self.svc,
            "op_search",
            return_value={
                "operation": "search",
                "prompt": "ammonia combustion kinetics",
                "raw": {"expanded_query": "ammonia combustion kinetics"},
                "new_papers": [
                    {"paper_id": "doi:10.1/a", "doi": "10.1/a", "title": "A", "source": "Bohrium"},
                ],
                "new_paper_count": 1,
                "query_id": "qid-1",
                "search_meta": {},
            },
        ), patch.object(
            self.svc,
            "_topup_seeds_from_crossref",
            return_value=(
                [{"paper_id": "doi:10.1/b", "doi": "10.1/b", "title": "B", "source": "crossref_query"}],
                {"error": None},
            ),
        ):
            s = self.svc.run_search(run_id)
        self.assertEqual(s["new_paper_count"], 2)

        with patch.object(
            self.svc,
            "op_classify",
            return_value={
                "operation": "relevance_classification",
                "topic": "ammonia combustion kinetics",
                "query_id": "qid-1",
                "counts": {"highly_relevant": 1, "closely_related": 1, "ignorable": 0, "non_classifiable": 0},
                "items_classified": 2,
                "items": [
                    {"doi": "10.1/a", "title": "A", "label": "highly_relevant"},
                    {"doi": "10.1/b", "title": "B", "label": "closely_related"},
                ],
            },
        ) as m_cls:
            c = self.svc.run_classify(run_id)
        self.assertEqual(c["items_classified"], 2)
        self.assertIsNotNone(m_cls.call_args.kwargs.get("candidates"))

        with patch.object(
            self.svc,
            "op_grow",
            return_value={"operation": "grow", "results": [{"level": 1, "discovered_count": 2}, {"level": 2, "discovered_count": 3}], "total_discovered_unique": 5},
        ):
            g = self.svc.run_grow(run_id)
        self.assertEqual(g["operation"], "grow")

        with patch.object(
            self.svc,
            "graph_rank",
            return_value={"seeds": ["doi:10.1/a"], "items": [{"doi": "10.1/c", "title": "C", "score": 0.9}]},
        ):
            r = self.svc.run_rank(run_id)
        self.assertEqual(r["operation"], "rank")

        rep = self.svc.run_report(run_id)
        self.assertTrue(Path(rep["report_md"]).exists())
        self.assertTrue(Path(rep["report_json"]).exists())
        self.assertTrue(Path(self.rm.run_dir(run_id) / "pool_scored.json").exists())
        self.assertTrue(Path(self.rm.run_dir(run_id) / "perf.json").exists())

        scored = self.rm.read_json(run_id, "pool_scored.json")
        self.assertTrue(scored.get("count", 0) >= 2)
        perf = self.rm.read_json(run_id, "perf.json")
        self.assertIn("stage_durations_ms", perf)

        history = Path(self.rm.run_dir(run_id) / "history.jsonl").read_text(encoding="utf-8")
        self.assertIn('"op": "search"', history)
        self.assertIn('"op": "classify"', history)
        self.assertIn('"op": "grow"', history)
        self.assertIn('"op": "rank"', history)
        self.assertIn('"op": "score"', history)
        self.assertIn('"op": "diagnostics"', history)
        self.assertIn('"op": "report"', history)


if __name__ == "__main__":
    unittest.main()
