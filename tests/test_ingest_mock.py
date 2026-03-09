import unittest

from papersearch.ingest.pipeline import discover_candidates, ingest_doi


class TestIngestMock(unittest.TestCase):
    def test_discover_mock(self):
        items = discover_candidates("quantum routing", limit=5, use_mock=True)
        self.assertEqual(len(items), 5)
        self.assertIn("doi", items[0])

    def test_ingest_elsevier_mock(self):
        out = ingest_doi("10.1016/j.mock.2024.123456", use_mock=True)
        self.assertTrue(out["elsevier_eligible"])
        self.assertEqual(out["fetch"]["status"], 200)
        self.assertIn("#", out["markdown"])
        self.assertTrue(out["quality"]["ok"])

    def test_ingest_non_elsevier_fallback(self):
        out = ingest_doi("10.48550/arXiv.2401.0001", title="Arxiv Paper", abstract="Some abstract", use_mock=True)
        self.assertFalse(out["elsevier_eligible"])
        self.assertEqual(out["fetch"]["source"], "fallback")
        self.assertIn("Arxiv Paper", out["markdown"])


if __name__ == "__main__":
    unittest.main()
