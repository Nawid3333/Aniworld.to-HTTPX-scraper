"""Critical regression tests for vanished-series cleanup and ignore persistence.

These tests exercise the new _find_vanished_to_clean / _prompt_clean_vanished /
_notify_vanished_at_startup helpers using temporary files only.  They never
read from or write to the real data/ directory.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
import unittest
from unittest.mock import patch

import config.config as cfg
import main


@contextlib.contextmanager
def _quiet():
    """Swallow the decision table's output so the suite stays readable."""
    with contextlib.redirect_stdout(io.StringIO()):
        yield


class VanishedCleanupTests(unittest.TestCase):
    """Verify cleanup logic and persistence in isolation."""

    def _make_index_entry(self, title: str, slug: str) -> dict:
        # Use a host and path shape that passes the project's URL validator.
        url = f"https://aniworld.to/anime/stream/{slug}"
        return {
            "title": title,
            "url": url,
            "total_seasons": 0,
            "total_episodes": 0,
            "watched_episodes": 0,
            "unwatched_episodes": 0,
            "seasons": [],
        }

    def _patch_paths(self, tmp: str):
        index_path = os.path.join(tmp, "series_index.json")
        mismatch_path = os.path.join(tmp, "mismatch_report.json")
        ignored_path = os.path.join(tmp, "ignored_vanished.json")

        patches = [
            patch.object(cfg, "SERIES_INDEX_FILE", index_path),
            patch.object(cfg, "DATA_DIR", tmp),
            patch.object(main, "SERIES_INDEX_FILE", index_path),
            patch.object(main, "DATA_DIR", tmp),
        ]
        for p in patches:
            p.start()
        self.addCleanup(lambda: [p.stop() for p in patches])
        return index_path, mismatch_path, ignored_path

    def _write_index(self, path: str, entries: list[dict]):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(entries, f)

    def _write_mismatch(self, path: str, only_in_index: list[str]):
        n = len(only_in_index)
        report = {
            "summary": {"added": 0, "removed": n, "changed": 0, "unchanged": 0, "total": n},
            "hosts": [
                {"host": "host1", "only_in_index": only_in_index, "missing_from_index": [], "mismatched": []},
                {"host": "host2", "only_in_index": only_in_index, "missing_from_index": [], "mismatched": []},
            ],
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(report, f)

    def test_no_mismatch_report_returns_empty(self):
        with tempfile.TemporaryDirectory(prefix="ani_vanished_") as tmp:
            index_path, mismatch_path, _ = self._patch_paths(tmp)
            self._write_index(index_path, [self._make_index_entry("Alpha", "alpha")])

            result = main._find_vanished_to_clean()
            self.assertEqual(result, {})

    def test_vanished_found_when_present_on_all_hosts(self):
        with tempfile.TemporaryDirectory(prefix="ani_vanished_") as tmp:
            index_path, mismatch_path, _ = self._patch_paths(tmp)
            self._write_index(index_path, [self._make_index_entry("Alpha", "alpha")])
            self._write_mismatch(mismatch_path, ["alpha"])

            result = main._find_vanished_to_clean()
            self.assertEqual(result, {"alpha": "Alpha"})

    def test_vanished_skipped_when_not_unanimous(self):
        with tempfile.TemporaryDirectory(prefix="ani_vanished_") as tmp:
            index_path, mismatch_path, _ = self._patch_paths(tmp)
            self._write_index(index_path, [self._make_index_entry("Alpha", "alpha")])
            report = {
                "summary": {"added": 0, "removed": 1, "changed": 0, "unchanged": 0, "total": 1},
                "hosts": [
                    {"host": "host1", "only_in_index": ["alpha"], "missing_from_index": [], "mismatched": []},
                    {"host": "host2", "only_in_index": [], "missing_from_index": [], "mismatched": []},
                ],
            }
            with open(mismatch_path, "w", encoding="utf-8") as f:
                json.dump(report, f)

            result = main._find_vanished_to_clean()
            self.assertEqual(result, {})

    def test_ignored_slugs_are_excluded(self):
        with tempfile.TemporaryDirectory(prefix="ani_vanished_") as tmp:
            index_path, mismatch_path, _ = self._patch_paths(tmp)
            self._write_index(index_path, [self._make_index_entry("Alpha", "alpha")])
            self._write_mismatch(mismatch_path, ["alpha"])

            result = main._find_vanished_to_clean(ignored={"alpha"})
            self.assertEqual(result, {})

    def test_prompt_decline_leaves_index_unchanged(self):
        with tempfile.TemporaryDirectory(prefix="ani_vanished_") as tmp:
            index_path, mismatch_path, _ = self._patch_paths(tmp)
            self._write_index(index_path, [self._make_index_entry("Alpha", "alpha")])
            self._write_mismatch(mismatch_path, ["alpha"])

            # The prompt now runs the vanished/new decision table, so "n"
            # answers the table's per-row question (keep) and then its
            # follow-up about silencing the entry (no).
            with patch("builtins.input", return_value="n"), _quiet():
                removed = main._prompt_clean_vanished()

            self.assertFalse(removed)
            with open(index_path, encoding="utf-8") as f:
                data = json.load(f)
            self.assertEqual(len(data), 1)
            self.assertEqual(data[0]["title"], "Alpha")

    def test_prompt_ignore_persists_slug(self):
        with tempfile.TemporaryDirectory(prefix="ani_vanished_") as tmp:
            index_path, mismatch_path, ignored_path = self._patch_paths(tmp)
            self._write_index(index_path, [self._make_index_entry("Alpha", "alpha")])
            self._write_mismatch(mismatch_path, ["alpha"])

            # Keep the entry in the table, then accept the offer to stop
            # reporting it.
            with patch("builtins.input", side_effect=["k", "y"]), _quiet():
                removed = main._prompt_clean_vanished()

            self.assertFalse(removed)
            with open(ignored_path, encoding="utf-8") as f:
                ignored = json.load(f)
            self.assertEqual(ignored, {"slugs": ["alpha"]})

    def test_prompt_confirm_removes_vanished(self):
        with tempfile.TemporaryDirectory(prefix="ani_vanished_") as tmp:
            index_path, mismatch_path, _ = self._patch_paths(tmp)
            self._write_index(
                index_path,
                [
                    self._make_index_entry("Alpha", "alpha"),
                    self._make_index_entry("Beta", "beta"),
                ],
            )
            self._write_mismatch(mismatch_path, ["alpha"])

            with patch("builtins.input", return_value="y"), _quiet():
                removed = main._prompt_clean_vanished()

            self.assertTrue(removed)
            with open(index_path, encoding="utf-8") as f:
                data = json.load(f)
            titles = {entry["title"] for entry in data}
            self.assertEqual(titles, {"Beta"})

    def test_notify_at_startup_prints_warning(self):
        with tempfile.TemporaryDirectory(prefix="ani_vanished_") as tmp:
            index_path, mismatch_path, _ = self._patch_paths(tmp)
            self._write_index(index_path, [self._make_index_entry("Alpha", "alpha")])
            self._write_mismatch(mismatch_path, ["alpha"])

            with patch("builtins.print") as mock_print:
                main._notify_vanished_at_startup()

            printed = " ".join(str(call.args[0]) for call in mock_print.call_args_list)
            self.assertIn("1 anime", printed)
            self.assertIn("not on any reachable host", printed)


if __name__ == "__main__":
    unittest.main()
