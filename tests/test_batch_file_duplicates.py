"""Duplicate lines in a batch file are listed, and deleted from it on a y.

The scrape has long collapsed two lines naming one anime -- "/anime/stream/x"
and "/anime/stream/x/staffel-2" -- but the file kept both, so every batch run
met them again. Option 5 now lists each such line under the one it repeats and asks
once whether to delete them. Only a y touches the file, and then only those
lines; the scrape skips the duplicates whatever the answer.

Run with:  python -m unittest discover -s tests
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

import main
from config.config import SITE_URL
from src import term
from tests._support import SERIES_PATH, captured_output

# What option 5 hands a batch file to, and the scraper that keys its lines.
ADD_BATCH = main.batch_add_from_file
SCRAPER_CLS = main.AniWorldScraper


def _url(slug: str, rest: str = "") -> str:
    return f"{SITE_URL}{SERIES_PATH}{slug}{rest}"


A, A_SEASON, B, C = _url("alpha"), _url("alpha", "/staffel-2"), _url("bravo"), _url("charlie")

# Line 5 repeats line 2's anime, line 7 repeats line 4 exactly; the comment,
# the blank line and the invalid line are not entries and are never touched.
BATCH = f"# my list\n{A}\n\n{B}\n{A_SEASON}\nnot-a-url\n{B}\n{C}\n"
WITHOUT_DUPLICATES = f"# my list\n{A}\n\n{B}\nnot-a-url\n{C}\n"


class _Answers:
    """Hand out *answers* one per prompt and record every prompt shown.

    Running out is a failure: a prompt asked more often than the test
    expects is exactly what these tests are here to see.
    """

    def __init__(self, *answers, on_prompt=None):
        self.answers = list(answers)
        self.asked: list[str] = []
        self.on_prompt = on_prompt

    def __call__(self, prompt=""):
        self.asked.append(prompt)
        if self.on_prompt:
            self.on_prompt(prompt)
        if not self.answers:
            raise AssertionError(f"asked again after the scripted answers ran out: {prompt!r}")
        answer = self.answers.pop(0)
        if isinstance(answer, BaseException):
            raise answer
        return answer


class BatchFileDuplicateTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.path = Path(tmp.name) / "series_urls.txt"

    def _write(self, text: str, newline: str = "\n") -> None:
        self.path.write_bytes(text.replace("\n", newline).encode("utf-8"))

    def _run(self, *answers, on_prompt=None, write_error=None):
        """Run the batch add; return (prompts shown, output, url_list it scraped or None)."""
        script = _Answers(*answers, on_prompt=on_prompt)
        with (
            mock.patch.object(main, "_run_scrape_and_save") as run,
            mock.patch.object(main, "_check_checkpoint", return_value={"ok": True, "resume": False}),
            mock.patch.object(main, "atomic_write_text", side_effect=write_error, wraps=main.atomic_write_text),
            mock.patch("builtins.input", script),
            captured_output() as out,
        ):
            ADD_BATCH(str(self.path))
        url_list = run.call_args.kwargs["run_kwargs"]["url_list"] if run.called else None
        return script.asked, term.strip_ansi(out.getvalue()), url_list

    def test_y_deletes_exactly_the_duplicate_lines(self):
        self._write(BATCH)
        asked, shown, _ = self._run("y", "n")
        self.assertEqual(self.path.read_text(encoding="utf-8"), WITHOUT_DUPLICATES)
        self.assertIn("Delete the 2 duplicate line(s)", asked[0])
        self.assertIn("Deleted 2 duplicate line(s)", shown)

    def test_each_duplicate_is_listed_under_the_line_it_repeats(self):
        self._write(BATCH)
        _, shown, _ = self._run("n", "n")
        for line in (
            f"Line 2: {A}  (kept)",
            f"Line 5: {A_SEASON}  (duplicate)",
            f"Line 4: {B}  (kept)",
            f"Line 7: {B}  (duplicate)",
        ):
            self.assertIn(line, shown)

    def test_n_leaves_the_file_byte_for_byte(self):
        self._write(BATCH)
        before = self.path.read_bytes()
        _, shown, _ = self._run("n", "n")
        self.assertEqual(self.path.read_bytes(), before)
        self.assertIn("left unchanged", shown)

    def test_end_of_input_deletes_nothing(self):
        self._write(BATCH)
        before = self.path.read_bytes()
        self._run(EOFError(), EOFError())
        self.assertEqual(self.path.read_bytes(), before)

    def test_the_scrape_skips_the_duplicates_whatever_the_answer(self):
        for answer in ("y", "n"):
            with self.subTest(answer=answer):
                self._write(BATCH)
                _, _, url_list = self._run(answer, "y")
                self.assertEqual(url_list, [A, B, C])

    def test_a_file_without_duplicates_is_not_asked_about(self):
        self._write(f"{A}\n{B}\n")
        asked, _, _ = self._run("n")
        self.assertEqual(len(asked), 1)
        self.assertIn("Proceed with batch add", asked[0])

    def test_crlf_line_endings_are_kept(self):
        self._write(BATCH, newline="\r\n")
        self._run("y", "n")
        self.assertEqual(self.path.read_bytes(), WITHOUT_DUPLICATES.replace("\n", "\r\n").encode("utf-8"))

    def test_a_duplicate_on_a_last_line_without_a_newline_is_deleted(self):
        self._write(f"{A}\n{B}\n{A}")
        self._run("y", "n")
        self.assertEqual(self.path.read_text(encoding="utf-8"), f"{A}\n{B}\n")

    def test_a_file_edited_while_the_prompt_was_open_is_not_overwritten(self):
        self._write(BATCH)
        edited = BATCH + f"{_url('delta')}\n"

        def edit_meanwhile(prompt):
            if "Delete" in prompt:
                self._write(edited)

        _, shown, _ = self._run("y", "n", on_prompt=edit_meanwhile)
        self.assertEqual(self.path.read_text(encoding="utf-8"), edited)
        self.assertIn("changed while the prompt was open", shown)

    def test_a_failed_write_is_reported_and_the_run_goes_on(self):
        self._write(BATCH)
        before = self.path.read_bytes()
        _, shown, url_list = self._run("y", "y", write_error=OSError("disk full"))
        self.assertEqual(self.path.read_bytes(), before)
        self.assertIn("Could not update", shown)
        self.assertEqual(url_list, [A, B, C])

    def test_the_file_and_the_scrape_agree_on_what_a_duplicate_is(self):
        scraper = SCRAPER_CLS()
        spellings = [A, A_SEASON, _url("ALPHA"), _url("alpha", "/staffel-2/episode-3")]
        self.assertEqual({scraper.batch_series_key(u) for u in spellings}, {scraper.batch_series_key(A)})
        self.assertEqual(len(scraper._series_list_from_urls(spellings)), 1)
        self.assertNotEqual(scraper.batch_series_key(A), scraper.batch_series_key(B))


if __name__ == "__main__":
    unittest.main()
