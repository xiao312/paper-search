import subprocess
import unittest
from unittest.mock import patch

from papersearch.integrations.pi_mono_client import PiMonoClient


class TestPiMonoClient(unittest.TestCase):
    def test_list_models_ok(self):
        cp = subprocess.CompletedProcess(args=["pi"], returncode=0, stdout="openai/gpt-4o\n", stderr="")
        with patch("papersearch.integrations.pi_mono_client.subprocess.run", return_value=cp):
            out = PiMonoClient().list_models(provider="openai")
        self.assertTrue(out.ok)
        self.assertIn("openai/gpt-4o", out.stdout)

    def test_prompt_timeout(self):
        with patch("papersearch.integrations.pi_mono_client.subprocess.run", side_effect=subprocess.TimeoutExpired(cmd=["pi"], timeout=1)):
            out = PiMonoClient(timeout_seconds=1).prompt("hi")
        self.assertFalse(out.ok)
        self.assertEqual(out.returncode, 124)


if __name__ == "__main__":
    unittest.main()
