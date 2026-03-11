import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from papersearch.app.repository import Repo
from papersearch.app.service import AppService, _merge_reference_rows


class TestGraphApiFirst(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        db = str(Path(self.tmp.name) / "test.db")
        self.repo = Repo(db_path=db)
        self.svc = AppService(repo=self.repo, notifier=None)

        self.repo.upsert_api_paper({
            "paper_id": "doi:10.1/a",
            "doi": "10.1/a",
            "openalex_id": None,
            "citation_count": 0,
            "title": "Paper A",
            "year": 2020,
            "venue": "J1",
            "abstract": "",
            "source": "test",
            "updated_at": "2026-01-01T00:00:00Z",
        })
        self.repo.upsert_api_paper({
            "paper_id": "doi:10.1/b",
            "doi": "10.1/b",
            "openalex_id": None,
            "citation_count": 0,
            "title": "Paper B",
            "year": 2021,
            "venue": "J1",
            "abstract": "",
            "source": "test",
            "updated_at": "2026-01-01T00:00:00Z",
        })
        self.repo.upsert_api_paper({
            "paper_id": "doi:10.1/c",
            "doi": "10.1/c",
            "openalex_id": None,
            "citation_count": 0,
            "title": "Paper C",
            "year": 2022,
            "venue": "J2",
            "abstract": "",
            "source": "test",
            "updated_at": "2026-01-01T00:00:00Z",
        })
        self.repo.upsert_api_paper({
            "paper_id": "doi:10.1/d",
            "doi": "10.1/d",
            "openalex_id": None,
            "citation_count": 0,
            "title": "Paper D",
            "year": 2023,
            "venue": "J2",
            "abstract": "",
            "source": "test",
            "updated_at": "2026-01-01T00:00:00Z",
        })

        self.repo.replace_api_references(
            "doi:10.1/a",
            [
                {"src_paper_id": "doi:10.1/a", "ref_order": 1, "doi": "10.1/b", "ref_openalex_id": None, "raw_text": "B"},
                {"src_paper_id": "doi:10.1/a", "ref_order": 2, "doi": "10.1/c", "ref_openalex_id": None, "raw_text": "C"},
            ],
        )
        self.repo.replace_api_references(
            "doi:10.1/d",
            [
                {"src_paper_id": "doi:10.1/d", "ref_order": 1, "doi": "10.1/c", "ref_openalex_id": None, "raw_text": "C"},
            ],
        )
        self.repo.replace_api_references(
            "doi:10.1/b",
            [
                {"src_paper_id": "doi:10.1/b", "ref_order": 1, "doi": "10.1/c", "ref_openalex_id": None, "raw_text": "C"},
            ],
        )
        self.repo.resolve_edges_doi_match("2026-01-01T00:00:00Z")

    def tearDown(self):
        self.tmp.cleanup()

    def test_graph_neighbors(self):
        out = self.svc.graph_neighbors("10.1/a", direction="out", limit=10)
        ids = {x["paper_id"] for x in out["neighbors"]["out"]}
        self.assertEqual(ids, {"doi:10.1/b", "doi:10.1/c"})

    def test_graph_related_coupling(self):
        out = self.svc.graph_related("doi:10.1/a", mode="coupling", limit=10)
        ids = [x["paper_id"] for x in out["items"]]
        self.assertIn("doi:10.1/b", ids)
        self.assertIn("doi:10.1/d", ids)

    def test_graph_related_cocite(self):
        out = self.svc.graph_related("doi:10.1/c", mode="cocite", limit=10)
        ids = [x["paper_id"] for x in out["items"]]
        self.assertIn("doi:10.1/b", ids)

    def test_graph_prior(self):
        out = self.svc.graph_prior("doi:10.1/c", direction="in", limit=10)
        ids = [x["paper_id"] for x in out["neighbors"]["in"]]
        self.assertIn("doi:10.1/a", ids)
        self.assertIn("doi:10.1/b", ids)
        self.assertNotIn("doi:10.1/d", ids)

    def test_graph_derivative(self):
        out = self.svc.graph_derivative("doi:10.1/c", direction="in", limit=10)
        ids = [x["paper_id"] for x in out["neighbors"]["in"]]
        self.assertIn("doi:10.1/d", ids)
        self.assertNotIn("doi:10.1/a", ids)

    def test_graph_related_set(self):
        out = self.svc.graph_related_set(["doi:10.1/a", "doi:10.1/d"], mode="coupling", limit=10)
        ids = [x["paper_id"] for x in out["items"]]
        self.assertIn("doi:10.1/b", ids)

    def test_merge_reference_rows_dedup(self):
        base = [{"doi": "10.1/x", "raw_text": "A"}, {"doi": None, "raw_text": "same text"}]
        extra = [{"doi": "10.1/x", "raw_text": "dup"}, {"doi": None, "raw_text": "same text"}, {"doi": "10.1/y", "raw_text": "B"}]
        out = _merge_reference_rows(base, extra)
        dois = [x["doi"] for x in out if x["doi"]]
        self.assertEqual(dois, ["10.1/x", "10.1/y"])

    def test_openalex_id_edge_resolution(self):
        self.repo.upsert_api_paper(
            {
                "paper_id": "doi:10.1/e",
                "doi": "10.1/e",
                "openalex_id": "W999",
                "citation_count": 0,
                "title": "Paper E",
                "year": 2024,
                "venue": "J3",
                "abstract": "",
                "source": "test",
                "updated_at": "2026-01-01T00:00:00Z",
            }
        )
        self.repo.replace_api_references(
            "doi:10.1/a",
            [
                {"src_paper_id": "doi:10.1/a", "ref_order": 1, "doi": None, "ref_openalex_id": "W999", "raw_text": ""},
            ],
        )
        self.repo.resolve_edges_doi_match("2026-01-01T00:00:00Z")
        out = self.svc.graph_neighbors("doi:10.1/a", direction="out", limit=20)
        ids = {x["paper_id"] for x in out["neighbors"]["out"]}
        self.assertIn("doi:10.1/e", ids)

    def test_graph_expand_from_missing_refs(self):
        self.repo.replace_api_references(
            "doi:10.1/c",
            [
                {"src_paper_id": "doi:10.1/c", "ref_order": 1, "doi": "10.1/x", "ref_openalex_id": None, "raw_text": ""},
                {"src_paper_id": "doi:10.1/c", "ref_order": 2, "doi": "10.1/y", "ref_openalex_id": None, "raw_text": ""},
            ],
        )

        def fake_ingest(doi: str, use_mock: bool = False):
            self.repo.upsert_api_paper(
                {
                    "paper_id": f"doi:{doi}",
                    "doi": doi,
                    "openalex_id": None,
                    "citation_count": 0,
                    "title": doi,
                    "year": 2025,
                    "venue": "JX",
                    "abstract": "",
                    "source": "test",
                    "updated_at": "2026-01-01T00:00:00Z",
                }
            )
            self.repo.replace_api_references(f"doi:{doi}", [])
            return {"paper_id": f"doi:{doi}", "doi": doi}

        with patch.object(self.svc, "graph_ingest_doi", side_effect=fake_ingest):
            out = self.svc.graph_expand(["doi:10.1/c"], rounds=1, max_new_per_round=10, max_workers=1)

        self.assertEqual(out["rounds"][0]["ingested"], 2)

    def test_graph_rank(self):
        out = self.svc.graph_rank(["doi:10.1/a"], limit=3)
        ids = [x["paper_id"] for x in out["items"]]
        self.assertIn("doi:10.1/c", ids)
        self.assertTrue(all(out["items"][i]["score"] >= out["items"][i + 1]["score"] for i in range(len(out["items"]) - 1)))
        self.assertIn("venue_boost", out["items"][0]["explain"])


if __name__ == "__main__":
    unittest.main()
