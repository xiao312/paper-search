import unittest

from papersearch.ingest.models import Document, Figure, Reference, Section
from papersearch.ingest.parse_elsevier_xml import parse_elsevier_xml
from papersearch.ingest.render_markdown import render_markdown


class TestIngestRefactor(unittest.TestCase):
    def test_reference_cleanup_and_dedup(self):
        xml = """<?xml version='1.0'?>
        <root>
          <bib-reference id='b1'>[1] Foo A. Bar B. Title X 2020 10.1234/abc A. Foo, B. Bar, Title X, Journal (2020).</bib-reference>
          <bib-reference id='b2'>[1] Foo A. Bar B. Title X 2020 10.1234/abc A. Foo, B. Bar, Title X, Journal (2020).</bib-reference>
        </root>
        """
        doc = parse_elsevier_xml(xml, doi="10.1016/j.mock.2024.1")
        self.assertEqual(len(doc.references), 1)
        self.assertTrue(doc.references[0].text.startswith("A. Foo"))

    def test_formula_spacing_cleanup(self):
        xml = """<?xml version='1.0'?>
        <root>
          <section><title>Intro</title><para>Model fails when ( ρ k &lt; 0 ) and uses ( θ ) in loss.</para></section>
        </root>
        """
        doc = parse_elsevier_xml(xml)
        text = doc.sections[0].paragraphs[0]
        self.assertIn("(ρ k < 0)", text)
        self.assertIn("(θ)", text)

    def test_figure_inserted_near_mention(self):
        doc = Document(
            title="T",
            sections=[Section(heading="Intro", paragraphs=["See Fig. 1 for overview."])],
            figures=[Figure(id="fig1", label="Fig. 1", caption="cap", asset_rel_path="assets/f1.jpg")],
            references=[Reference(text="R1")],
        )
        md = render_markdown(doc)
        mention_i = md.find("See Fig. 1")
        fig_i = md.find("### Fig. 1")
        ref_i = md.find("## References")
        self.assertTrue(mention_i >= 0 and fig_i > mention_i and fig_i < ref_i)
        self.assertIn("![Fig. 1](assets/f1.jpg)", md)

    def test_prefers_relative_asset_path(self):
        doc = Document(
            title="T",
            sections=[],
            figures=[Figure(id="fig1", label="Fig. 1", asset_local_path="/abs/a.jpg", asset_rel_path="assets/a.jpg")],
        )
        md = render_markdown(doc)
        self.assertIn("![Fig. 1](assets/a.jpg)", md)
        self.assertNotIn("/abs/a.jpg", md)

    def test_metadata_rendered(self):
        doc = Document(
            title="T",
            metadata={
                "doi": "10.1016/j.test.1",
                "authors": ["A. One", "B. Two"],
                "affiliations": ["Institute X"],
                "journal": "Fuel",
                "cover_date": "2026-01-01",
            },
        )
        md = render_markdown(doc)
        self.assertIn("## Metadata", md)
        self.assertIn("**DOI:** 10.1016/j.test.1", md)
        self.assertIn("**Authors:**", md)
        self.assertIn("  - A. One", md)
        self.assertIn("  - B. Two", md)
        self.assertIn("Institute X", md)


if __name__ == "__main__":
    unittest.main()
