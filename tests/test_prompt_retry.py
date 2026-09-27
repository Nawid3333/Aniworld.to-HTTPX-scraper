"""A mistyped answer is asked again; only the answers a prompt offers count.

Every y/n prompt used to read anything but "y" as no. Typing "sy" at "Add these
new series to the index?" declined them, the run found nothing left to save,
and the user was back at the main menu with the scrape thrown away. Menus did
the same with other fallbacks: a typo at the integrity dialog proceeded with
the merge, and a missing file at "batch add" went back to the main menu.

term.confirm and term.ask now take only what the prompt shows -- y or n in
either case, or a listed option -- and ask again on anything else. There are
no defaults: Enter alone is never an answer. End of input, or MAX_UNRECOGNIZED
wrong answers in a row, gives the answer that changes nothing, so an
unattended run cannot loop forever.
"""

from __future__ import annotations

import ast
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import main
from src import index_manager as im
from src import term
from tests._support import captured_output, series, write_index

REPO = Path(__file__).resolve().parent.parent

# What option 5 hands a URL and a batch file to.
ADD_SINGLE = "add_single_series"
ADD_BATCH = "batch_add_from_file"

ALLOW_EVERYTHING = dict.fromkeys(
    (
        "new_series",
        "new_episodes",
        "watched",
        "unwatched",
        "subscribe",
        "unsubscribe",
        "watchlist_add",
        "watchlist_remove",
        "title_ger",
        "title_eng",
        "episode_remove",
        "season_remove",
    ),
    True,
)


class _Script:
    """Hand out *answers* one per prompt and record every prompt shown.

    Running out is a failure, not an end of input: a prompt that asks more
    often than the test expects is exactly what these tests are here to see.
    """

    def __init__(self, *answers):
        self.answers = list(answers)
        self.asked: list[str] = []

    def __call__(self, prompt=""):
        self.asked.append(prompt)
        if not self.answers:
            raise AssertionError(f"asked again after the scripted answers ran out: {prompt!r}")
        return self.answers.pop(0)


def _confirm(*answers):
    script = _Script(*answers)
    with mock.patch("builtins.input", script), captured_output() as out:
        result = term.confirm("Go? (y/n): ")
    return result, script.asked, term.strip_ansi(out.getvalue())


def _ask(*answers):
    script = _Script(*answers)
    with mock.patch("builtins.input", script), captured_output() as out:
        result = term.ask("Choose (0-2): ", ("0", "1", "2"), safe="0")
    return result, script.asked, term.strip_ansi(out.getvalue())


class ConfirmTests(unittest.TestCase):
    def test_y_and_n_answer_in_either_case(self):
        for answer, expected in (("y", True), ("Y", True), ("n", False), ("N", False)):
            with self.subTest(answer=answer):
                result, asked, _ = _confirm(answer)
                self.assertIs(result, expected)
                self.assertEqual(len(asked), 1)

    def test_a_typo_is_asked_again_and_the_next_answer_counts(self):
        # The incident: "sy" at "Add these new series to the index?".
        result, asked, shown = _confirm("sy", "y")
        self.assertTrue(result)
        self.assertEqual(len(asked), 2)
        self.assertIn("'sy' is not an option - type y or n.", shown)

    def test_nothing_but_y_or_n_is_accepted(self):
        for wrong in ("yes", "no", "j", "ja", "nein", "yn", "ny", "1", "0", "q", "k"):
            for then, expected in (("y", True), ("n", False)):
                with self.subTest(wrong=wrong, then=then):
                    result, asked, _ = _confirm(wrong, then)
                    self.assertIs(result, expected)
                    self.assertEqual(len(asked), 2, "a wrong answer was taken instead of asked again")

    def test_enter_alone_is_not_an_answer(self):
        for blank in ("", "   "):
            with self.subTest(blank=repr(blank)):
                result, asked, shown = _confirm(blank, "n")
                self.assertFalse(result)
                self.assertEqual(len(asked), 2)
                self.assertIn("No answer - type y or n.", shown)

    def test_spaces_around_the_answer_are_ignored(self):
        self.assertTrue(_confirm(" y ")[0])
        self.assertFalse(_confirm("n ")[0])

    def test_a_right_answer_after_several_wrong_ones_still_counts(self):
        result, asked, _ = _confirm(*["x"] * (term.MAX_UNRECOGNIZED - 1), "y")
        self.assertTrue(result)
        self.assertEqual(len(asked), term.MAX_UNRECOGNIZED)

    def test_end_of_input_answers_no(self):
        with mock.patch("builtins.input", side_effect=EOFError), captured_output():
            self.assertFalse(term.confirm("Go? (y/n): "))

    def test_endless_wrong_answers_stop_and_answer_no(self):
        feed = mock.Mock(return_value="x")
        with mock.patch("builtins.input", feed), captured_output() as out:
            self.assertFalse(term.confirm("Go? (y/n): "))
        self.assertEqual(feed.call_count, term.MAX_UNRECOGNIZED)
        self.assertIn(f"No usable answer after {term.MAX_UNRECOGNIZED} tries", term.strip_ansi(out.getvalue()))


class AskTests(unittest.TestCase):
    def test_a_listed_option_is_returned(self):
        for answer in ("0", "1", "2"):
            with self.subTest(answer=answer):
                self.assertEqual(_ask(answer)[0], answer)

    def test_an_unlisted_answer_is_asked_again(self):
        for wrong in ("3", "12", "x", "-1", "1-2", "01"):
            with self.subTest(wrong=wrong):
                result, asked, shown = _ask(wrong, "1")
                self.assertEqual(result, "1")
                self.assertEqual(len(asked), 2)
                self.assertIn(f"{wrong!r} is not an option - type one of 0, 1, 2.", shown)

    def test_enter_is_never_an_answer(self):
        result, asked, shown = _ask("", "1")
        self.assertEqual((result, len(asked)), ("1", 2))
        self.assertIn("No answer - type one of 0, 1, 2.", shown)

    def test_letters_match_in_either_case(self):
        with mock.patch("builtins.input", _Script("D")), captured_output():
            self.assertEqual(term.ask("? ", ("d", "s"), safe="s"), "d")

    def test_end_of_input_gives_the_safe_answer(self):
        with mock.patch("builtins.input", side_effect=EOFError), captured_output():
            self.assertEqual(term.ask("? ", ("1", "2", "3"), safe="3"), "3")

    def test_endless_wrong_answers_give_the_safe_answer(self):
        feed = mock.Mock(return_value="x")
        with mock.patch("builtins.input", feed), captured_output():
            self.assertEqual(term.ask("? ", ("1", "2", "3"), safe="3"), "3")
        self.assertEqual(feed.call_count, term.MAX_UNRECOGNIZED)


class PromptSiteTests(unittest.TestCase):
    """A typo followed by the right answer does what the right answer says."""

    def test_a_typo_at_the_new_series_prompt_is_asked_again(self):
        # The exact incident: "sy" there used to decline the new series.
        for then, expected in (("y", True), ("n", False)):
            with self.subTest(then=then):
                changes = im.detect_changes({}, {})
                changes["new_series"] = ["A"]
                script = _Script("sy", then)
                with mock.patch("builtins.input", script), captured_output():
                    allowed = im._prompt_change_confirmations(changes, {"A": series("A")})
                self.assertIs(allowed["new_series"], expected)
                self.assertEqual(len(script.asked), 2)

    def test_a_typo_at_the_save_prompt_is_asked_again(self):
        for then, saved in (("y", ["Added", "Kept"]), ("n", ["Kept"])):
            with self.subTest(then=then):
                path = write_index([series("Kept", slug="kept")])
                manager = im.IndexManager(path)
                with (
                    mock.patch.object(im, "_prompt_change_confirmations", return_value=dict(ALLOW_EVERYTHING)),
                    mock.patch("builtins.input", _Script("sy", then)),
                    captured_output(),
                ):
                    im.confirm_and_save_changes(
                        [series("Kept", slug="kept"), series("Added", slug="added")], "test", manager
                    )
                with open(path, encoding="utf-8") as f:
                    self.assertEqual(sorted(e["title"] for e in json.load(f)), saved)

    def test_a_typo_at_the_rename_prompt_is_asked_again(self):
        old = im._key_series([series("Old Name", slug="same")])
        new = im._key_series([series("New Name", slug="same")])
        with mock.patch("builtins.input", _Script("sy", "y")), captured_output():
            renamed = im._prompt_title_renames(im._find_title_renames(old, new), old, new)
        self.assertEqual(renamed, [("Old Name", "New Name")])
        self.assertEqual(list(old), ["New Name"])

    def test_enter_is_not_keep_in_the_vanished_table(self):
        vanished = [("Gone", "not found", series("Gone", slug="gone")["url"])]
        script = _Script("", "k")
        with mock.patch("builtins.input", script), captured_output():
            self.assertEqual(im._prompt_vanished_table(vanished, {}, {}), [])
        self.assertEqual(len(script.asked), 2)


class BatchAddPromptTests(unittest.TestCase):
    """Option 5: "1" is the default file now, and a mistake is asked again."""

    def setUp(self):
        self.batch_file = Path(tempfile.mkdtemp()) / "series_urls.txt"
        self.batch_file.write_text("", encoding="utf-8")

    def _run(self, *answers):
        script = _Script(*answers)
        with (
            mock.patch.object(main, "DEFAULT_BATCH_FILE", str(self.batch_file)),
            mock.patch.object(main, ADD_SINGLE) as single,
            mock.patch.object(main, ADD_BATCH) as batch,
            mock.patch("builtins.input", script),
            captured_output() as out,
        ):
            main.single_or_batch_add()
        return single, batch, script.asked, term.strip_ansi(out.getvalue())

    def test_1_uses_the_default_file(self):
        single, batch, asked, _ = self._run("1")
        batch.assert_called_once_with(str(self.batch_file))
        single.assert_not_called()
        self.assertEqual(len(asked), 1)

    def test_enter_is_asked_again_rather_than_meaning_the_default_file(self):
        single, batch, asked, shown = self._run("", "1")
        self.assertEqual(len(asked), 2)
        self.assertIn("No answer", shown)
        batch.assert_called_once_with(str(self.batch_file))

    def test_a_missing_file_is_asked_again(self):
        _, batch, asked, shown = self._run("no-such-file.txt", "1")
        self.assertIn("File not found: no-such-file.txt", shown)
        self.assertEqual(len(asked), 2)
        batch.assert_called_once_with(str(self.batch_file))

    def test_a_url_that_is_not_a_series_page_is_asked_again(self):
        url = series("Dark", slug="dark")["url"]
        single, batch, asked, _ = self._run("https://example.com/nothing", url)
        single.assert_called_once_with(url)
        batch.assert_not_called()
        self.assertEqual(len(asked), 2)

    def test_0_goes_back_without_doing_anything(self):
        single, batch, asked, _ = self._run("no-such-file.txt", "0")
        single.assert_not_called()
        batch.assert_not_called()
        self.assertEqual(len(asked), 2)

    def test_endless_mistakes_go_back_to_the_menu(self):
        feed = mock.Mock(return_value="no-such-file.txt")
        with (
            mock.patch.object(main, ADD_SINGLE) as single,
            mock.patch.object(main, ADD_BATCH) as batch,
            mock.patch("builtins.input", feed),
            captured_output(),
        ):
            main.single_or_batch_add()
        self.assertEqual(feed.call_count, term.MAX_UNRECOGNIZED)
        single.assert_not_called()
        batch.assert_not_called()


def _docstring_ids(tree):
    """Return the ids of every docstring node; a docstring may describe a prompt."""
    ids = set()
    for node in ast.walk(tree):
        body = getattr(node, "body", None)
        if isinstance(body, list) and body and isinstance(body[0], ast.Expr):
            ids.add(id(body[0].value))
    return ids


def _strings(path):
    """Yield (node, text) for each string constant in *path* that is not a docstring."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    exempt = _docstring_ids(tree)
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "confirm":
            exempt.update(id(inner) for inner in ast.walk(node))
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in exempt:
            yield node, node.value


def _all_strings(path):
    """Yield (node, text) for every string constant in *path* except docstrings."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    exempt = _docstring_ids(tree)
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in exempt:
            yield node, node.value


def _yn_prompts_outside_confirm(path):
    """Return "file:line" for each "(y/n)" string not handed to term.confirm."""
    return [f"{path.name}:{node.lineno}" for node, text in _strings(path) if "(y/n)" in text]


# How a prompt used to offer an answer for Enter.
_DEFAULT_MARKERS = ("[default", "[k]", "[n]", "[y]", "Enter = cancel", "Press Enter")


def _offered_defaults(path):
    """Return "file:line" for each string that offers Enter an answer."""
    return [
        f"{path.name}:{node.lineno}"
        for node, text in _all_strings(path)
        if any(marker in text for marker in _DEFAULT_MARKERS)
    ]


class SourceGuardTests(unittest.TestCase):
    """Neither rule can quietly come back in a later change."""

    sources = [REPO / "main.py", *sorted((REPO / "src").glob("*.py"))]

    def test_every_y_n_prompt_uses_term_confirm(self):
        offenders = [hit for path in self.sources for hit in _yn_prompts_outside_confirm(path)]
        self.assertEqual(offenders, [], "a y/n prompt reads input directly; use term.confirm")

    def test_no_prompt_offers_a_default(self):
        offenders = [hit for path in self.sources for hit in _offered_defaults(path)]
        self.assertEqual(offenders, [], "a prompt offers Enter an answer; make every answer typed")

    def test_the_guards_see_what_they_are_meant_to(self):
        planted = Path(tempfile.mkdtemp()) / "planted.py"
        planted.write_text(
            'def f():\n    """Asks (y/n) [n] in a docstring - allowed."""\n'
            '    return input("Delete all? (y/n) [n]: ") == "y"\n',
            encoding="utf-8",
        )
        self.assertEqual(_yn_prompts_outside_confirm(planted), ["planted.py:3"])
        self.assertEqual(_offered_defaults(planted), ["planted.py:3"])


if __name__ == "__main__":
    unittest.main()
