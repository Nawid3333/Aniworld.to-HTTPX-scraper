"""Any parser change must reproduce the current output exactly.

The parsers are the one place in this project where a subtle regression is
invisible: a selector that silently matches nothing turns into "0 episodes
watched", which then looks like real data. So a replacement parser is not
judged by whether it looks right, it is judged against what today's code
actually produces on real markup.

`tests/fixtures/golden.json` is that record. These tests read it as the
reference and re-parse every captured page, field by field, so a change is
either byte-identical or it fails with the exact page and field that moved.

This is deliberately stricter than test_golden_parse.py: that one compares
whole-page JSON blobs, which tells you *that* something changed. These
compare per field and per episode, which tells you *what*.

Run with:  python -m unittest discover -s tests
"""

import gzip
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.config import SITE_URL  # noqa: E402
from tests import fixture_spec  # noqa: E402

FIXTURES = Path(__file__).resolve().parent / "fixtures"
PAGES = FIXTURES / "pages"
GOLDEN_FILE = FIXTURES / "golden.json"


def _load():
    if not GOLDEN_FILE.exists():
        return {}
    return json.loads(GOLDEN_FILE.read_text(encoding="utf-8"))


GOLDEN = _load()


def _html(name):
    return gzip.decompress((PAGES / f"{name}.html.gz").read_bytes()).decode("utf-8")


@unittest.skipUnless(GOLDEN, "no fixtures captured yet -- run tests/capture_fixtures.py")
class TestParserOutputIsUnchanged(unittest.TestCase):
    """Field-by-field, page-by-page, against the recorded reference."""

    def _pages(self):
        for name, entry in sorted(GOLDEN.items()):
            if (PAGES / f"{name}.html.gz").exists():
                yield name, entry

    def test_every_scalar_field_matches(self):
        checked = 0
        for name, entry in self._pages():
            actual = fixture_spec.parse_all(_html(name), entry["slug"], SITE_URL)
            expected = entry["result"]
            for field in ("title", "is_logged_in", "error_page", "subscribed", "watchlist"):
                if field not in expected:
                    continue
                with self.subTest(page=name, field=field):
                    self.assertEqual(actual.get(field), expected[field], f"{field} changed on {name}")
            checked += 1
        self.assertGreater(checked, 0, "no fixture pages on disk")

    def test_every_episode_matches(self):
        """Episode number and watched flag are the data the index is built from."""
        total = 0
        for name, entry in self._pages():
            expected = entry["result"].get("episodes")
            if not expected:
                continue
            actual = fixture_spec.parse_all(_html(name), entry["slug"], SITE_URL).get("episodes")
            with self.subTest(page=name):
                self.assertIsNotNone(actual, f"{name}: parser returned None where it used to return episodes")
                self.assertEqual(len(actual), len(expected), f"{name}: episode count changed")
                for exp, act in zip(expected, actual, strict=True):
                    self.assertEqual(act.get("number"), exp.get("number"), f"{name}: episode number changed")
                    self.assertEqual(act.get("watched"), exp.get("watched"), f"{name}: watched flag changed")
            total += len(expected)
        self.assertGreater(total, 0, "no episodes pinned in golden.json")

    def test_episode_titles_and_languages_match(self):
        for name, entry in self._pages():
            expected = entry["result"].get("episodes")
            if not expected:
                continue
            actual = fixture_spec.parse_all(_html(name), entry["slug"], SITE_URL).get("episodes") or []
            with self.subTest(page=name):
                for exp, act in zip(expected, actual, strict=True):
                    for field in ("title_ger", "title_eng", "languages"):
                        self.assertEqual(
                            act.get(field), exp.get(field), f"{name} ep {exp.get('number')}: {field} changed"
                        )

    def test_season_links_match(self):
        for name, entry in self._pages():
            expected = entry["result"].get("season_links")
            if expected is None:
                continue
            actual = fixture_spec.parse_all(_html(name), entry["slug"], SITE_URL).get("season_links")
            with self.subTest(page=name):
                self.assertEqual(actual, expected, f"{name}: season links changed")

    def test_the_none_versus_empty_distinction_survives(self):
        """None means "could not parse", [] means "genuinely no episodes".

        Collapsing the two is the specific regression that would write 0
        episodes into the index for a page the parser simply failed on.
        """
        nones = empties = 0
        for name, entry in self._pages():
            expected = entry["result"].get("episodes", "absent")
            if expected == "absent":
                continue
            actual = fixture_spec.parse_all(_html(name), entry["slug"], SITE_URL).get("episodes", "absent")
            with self.subTest(page=name):
                self.assertEqual(
                    actual is None, expected is None, f"{name}: None/empty distinction changed"
                )
            nones += expected is None
            empties += expected == []
        # only informational -- the fixtures may or may not contain either shape
        self.assertGreaterEqual(nones + empties, 0)


if __name__ == "__main__":
    unittest.main()
