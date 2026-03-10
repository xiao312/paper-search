import json
import unittest
from unittest.mock import patch

from papersearch.ingest.discovery_crossref import CrossrefClient


class TestCrossrefClient(unittest.TestCase):
    def test_search_works_parse(self):
        payload = {
            "message": {
                "total-results": 1,
                "items": [
                    {
                        "DOI": "10.1016/j.fuel.2026.138904",
                        "title": ["Enhancing deep learning..."],
                        "container-title": ["Fuel"],
                        "issued": {"date-parts": [[2026, 9, 1]]},
                        "score": 120.0,
                        "type": "journal-article",
                        "URL": "https://doi.org/10.1016/j.fuel.2026.138904",
                        "is-referenced-by-count": 0,
                    }
                ],
            }
        }

        with patch("papersearch.ingest.discovery_crossref.get_json_with_retry", return_value=payload):
            out = CrossrefClient().search_works("ammonia combustion", rows=30)

        self.assertIsNone(out["error"])
        self.assertEqual(len(out["items"]), 1)
        self.assertEqual(out["items"][0]["doi"], "10.1016/j.fuel.2026.138904")


if __name__ == "__main__":
    unittest.main()
