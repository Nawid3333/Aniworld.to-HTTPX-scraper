"""Golden-fixture parser tests: every optimisation must not change output.

`capture_fixtures.py` stores real pages from the live site alongside exactly
what today's parsers produce for them. These tests re-parse those pages and
demand an identical result, field for field. That is what makes swapping
parsers, restricting the parsed subtree, or moving parsing onto a thread a
safe, checkable change rather than a hopeful one.

Skips itself when no fixtures have been captured yet.
"""

import gzip
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.config import SITE_URL  # noqa: E402
from tests import fixture_spec  # noqa: E402

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures"
PAGE_DIR = FIXTURE_DIR / "pages"
GOLDEN_FILE = FIXTURE_DIR / "golden.json"


def _load_golden() -> dict:
    if not GOLDEN_FILE.exists():
        return {}
    with open(GOLDEN_FILE, encoding="utf-8") as fh:
        return json.load(fh)


GOLDEN = _load_golden()


@unittest.skipUnless(GOLDEN, "no fixtures captured yet -- run tests/capture_fixtures.py")
class TestGoldenParse(unittest.TestCase):
    def test_every_fixture_parses_identically(self):
        checked = 0
        for name, expected in GOLDEN.items():
            page = PAGE_DIR / f"{name}.html.gz"
            if not page.exists():
                continue
            html = gzip.decompress(page.read_bytes()).decode("utf-8")
            with self.subTest(page=name):
                actual = fixture_spec.parse_all(html, expected["slug"], SITE_URL)
                self.assertEqual(
                    json.dumps(actual, sort_keys=True, ensure_ascii=False),
                    json.dumps(expected["result"], sort_keys=True, ensure_ascii=False),
                    f"parser output changed for {name}",
                )
            checked += 1
        self.assertGreater(checked, 0, "fixtures listed in golden.json but no pages on disk")

    def test_fixtures_cover_the_shapes_that_matter(self):
        """A harness that only ever saw one kind of page proves very little."""
        names = list(GOLDEN)
        self.assertTrue(any(n.startswith("series__") for n in names), "no series pages captured")
        self.assertTrue(any(n.startswith("season__") for n in names), "no season pages captured")
        episode_counts = [
            len(v["result"].get("episodes") or []) for k, v in GOLDEN.items() if k.startswith("season__")
        ]
        self.assertTrue(any(c > 0 for c in episode_counts), "no captured season page has episodes")


if __name__ == "__main__":
    unittest.main()
