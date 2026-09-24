"""The decisions themselves: which answer opens which gate, and what gets removed.

Most suites mock the approval prompts away to test what happens *after* a
decision. That left the prompts untested, and they are where a wrong mapping
would do the most damage: one mixed-up key and an answer of "n" to deleting
episodes becomes a yes. The same goes for the catalogue parse, which decides
what a new-only scrape treats as new, and for the prompts that delete entries.
"""

from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import main
from src import index_manager as im
from src.scraper import AniWorldScraper
from tests._support import FakeResponse, captured_output, series, write_index

ALL_GATES = (
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
)

# change category -> (sample entry, words from its prompt, gate it opens)
CATEGORIES = {
    "new_series": ("A", "Add these new series", "new_series"),
    "new_episodes": (("A", "Season 1", 1), "Add these new episodes", "new_episodes"),
    "newly_watched": (("A", "Season 1", 1), "marked as WATCHED", "watched"),
    "newly_unwatched": (("A", "Season 1", 1), "marked as UNWATCHED", "unwatched"),
    "newly_subscribed": ("A", "subscription/watchlist", "subscribe"),
    "newly_unsubscribed": ("A", "subscription/watchlist", "unsubscribe"),
    "watchlist_added": ("A", "subscription/watchlist", "watchlist_add"),
    "watchlist_removed": ("A", "subscription/watchlist", "watchlist_remove"),
    "title_ger_changed": (("A", "Alt", "Neu"), "German title", "title_ger"),
    "title_eng_changed": (("A", "Old", "New"), "English title", "title_eng"),
    "removed_episodes": (("A", "Season 1", 1), "DELETE these episodes", "episode_remove"),
    "removed_seasons": (("A", "Season 1"), "DELETE these whole seasons", "season_remove"),
}


def _changes(*present):
    changes = {category: [] for category in CATEGORIES}
    for category in present:
        changes[category] = [CATEGORIES[category][0]]
    return changes


class _Answers:
    """Answer "y" to prompts containing any of *yes_to*, and *default* otherwise."""

    def __init__(self, *yes_to, default="n"):
        self.yes_to = yes_to
        self.default = default
        self.asked: list[str] = []

    def __call__(self, prompt=""):
        self.asked.append(prompt)
        return "y" if any(words in prompt for words in self.yes_to) else self.default


def _confirm(changes, answers):
    with mock.patch("builtins.input", answers), captured_output():
        return im._prompt_change_confirmations(changes, {"A": series("A")})


class ChangeConfirmationTests(unittest.TestCase):
    def test_yes_opens_exactly_its_own_gate(self):
        for category, (_sample, words, gate) in CATEGORIES.items():
            with self.subTest(category):
                allowed = _confirm(_changes(category), _Answers(words))
                self.assertTrue(allowed[gate])
                self.assertEqual([g for g in ALL_GATES if allowed[g]], [gate])

    def test_anything_but_yes_keeps_every_gate_closed(self):
        # "j" is the German yes; it is not accepted, and that is the safe way
        # round -- a refused change is simply offered again next scrape.
        for answer in ("", "n", "no", "j", "ja", "yes please"):
            with self.subTest(answer=answer):
                allowed = _confirm(_changes(*CATEGORIES), _Answers(default=answer))
                self.assertEqual([g for g in ALL_GATES if allowed[g]], [])

    def test_the_shared_subscription_answer_opens_only_the_directions_present(self):
        allowed = _confirm(_changes("newly_subscribed"), _Answers("subscription/watchlist"))
        self.assertTrue(allowed["subscribe"])
        self.assertFalse(allowed["unsubscribe"] or allowed["watchlist_add"] or allowed["watchlist_remove"])

    def test_a_declined_new_series_is_not_asked_about_again(self):
        answers = _Answers()
        _confirm(_changes("new_series", "newly_watched"), answers)
        self.assertTrue(any("new series" in p for p in answers.asked))
        self.assertFalse(any("WATCHED" in p for p in answers.asked), "watch state of a refused series was asked")


def _mismatch(severity, title="A"):
    return {
        "title": title,
        "severity": severity,
        "issues": [{"type": "total_episode_count", "old": 12, "new": 10, "diff": -2, "percent_diff": -16.7}],
    }


class EpisodeMismatchDialogTests(unittest.TestCase):
    """The integrity dialog, including the path that deletes and re-scrapes."""

    def setUp(self):
        # The dialog appends to logs/integrity_check.log next to the package;
        # point that at a temp dir so tests never write into the real log.
        tmp = tempfile.mkdtemp()
        patcher = mock.patch.object(im, "__file__", str(Path(tmp) / "src" / "index_manager.py"))
        patcher.start()
        self.addCleanup(patcher.stop)
        entry = series("A", slug="a")
        entry["url"] = entry["link"] = "/anime/stream/a"
        self.old_data = {"A": entry}

    def _run(self, mismatches, *answers):
        with mock.patch("builtins.input", side_effect=list(answers)), captured_output():
            return im._prompt_episode_mismatches(mismatches, self.old_data, active_site_url="https://mirror.example/")

    def test_info_only_is_approved_without_asking(self):
        self.assertEqual(self._run([_mismatch("info")]), (True, None))

    def test_warnings_need_an_explicit_yes(self):
        self.assertEqual(self._run([_mismatch("warning")], "y"), (True, None))
        for answer in ("", "n", "j"):
            with self.subTest(answer=answer):
                self.assertEqual(self._run([_mismatch("warning")], answer), (False, None))

    def test_cancel_discards_the_merge(self):
        self.assertEqual(self._run([_mismatch("critical")], "3"), (False, None))

    def test_rescrape_hands_back_the_entry_and_an_absolute_url(self):
        proceed, data = self._run([_mismatch("critical")], "2")
        self.assertFalse(proceed)
        assert data is not None
        self.assertEqual(data["urls"], ["https://mirror.example/anime/stream/a"])
        self.assertEqual(data["titles"], ["A"])
        self.assertIs(data["series"]["A"], self.old_data["A"])

    def test_rescrape_of_an_entry_that_cannot_be_found_gives_up(self):
        self.assertEqual(self._run([_mismatch("critical", title="Unknown")], "2"), (False, None))

    def test_enter_proceeds_and_leaves_every_deletion_to_its_own_prompt(self):
        # Proceeding is not destructive: the merge still asks separately before
        # removing any episode or season, and those prompts default to keep.
        self.assertEqual(self._run([_mismatch("critical")], ""), (True, None))


def _index_with(*entries):
    path = write_index(list(entries))
    return path, im.IndexManager(path)


def _titles_on_disk(path):
    with open(path, encoding="utf-8") as f:
        return sorted(e["title"] for e in json.load(f))


class DuplicateSlugPromptTests(unittest.TestCase):
    """main._remove_duplicate_index_entries deletes; only what the user picked may go."""

    def setUp(self):
        self.path, self.manager = _index_with(
            series("Old Name", slug="shared", watched=5),
            series("New Name", slug="shared"),
            series("Other", slug="other", watched=2),
        )

    def _resolve(self, *answers):
        _slugs, duplicates, _missing = main._collect_index_slugs(self.manager)
        with mock.patch("builtins.input", side_effect=list(answers)), captured_output():
            main._remove_duplicate_index_entries(self.manager, duplicates)

    def test_keeping_one_copy_removes_only_its_siblings(self):
        self._resolve("2")  # listed alphabetically: 1 = New Name, 2 = Old Name
        self.assertEqual(_titles_on_disk(self.path), ["Old Name", "Other"])

    def test_skip_abort_and_unknown_answers_keep_every_copy(self):
        for answer in ("", "s", "a", "3", "x"):
            with self.subTest(answer=answer):
                self._resolve(answer)
                self.assertEqual(_titles_on_disk(self.path), ["New Name", "Old Name", "Other"])

    def test_abort_stops_before_later_slugs(self):
        path, manager = _index_with(
            series("A1", slug="a"), series("A2", slug="a"), series("B1", slug="b"), series("B2", slug="b")
        )
        _slugs, duplicates, _missing = main._collect_index_slugs(manager)
        with mock.patch("builtins.input", side_effect=["a", "1"]), captured_output():
            main._remove_duplicate_index_entries(manager, duplicates)
        self.assertEqual(_titles_on_disk(path), ["A1", "A2", "B1", "B2"])


CATALOGUE = """<html><body><div class="avatar"><a href="/user/profil/me">me</a></div>
<div id="seriesContainer"><ul>
<li><a href="/anime/stream/waldern" data-alternative-title="The Missing Girl">Wäldern</a></li>
<li><a href="/anime/stream/wldern">Wäldern</a></li>
<li><a href="/anime/stream/25%20Years%20of%20You">25 Years of You</a></li>
<li><a href="/anime/stream/25%20years%20of%20you">25 Years of You</a></li>
<li><a href="/anime/stream/dark/staffel-1">Dark Staffel 1</a></li>
</ul></div>
<nav><a href="/anime/stream/from-the-menu">From The Menu</a></nav>
</body></html>"""


def _parse_catalogue(html):
    scraper = AniWorldScraper()

    async def fake_get(client, url, *args, **kwargs):
        return FakeResponse(200, html)

    scraper._get = fake_get  # type: ignore[method-assign]
    return asyncio.run(scraper._get_all_series(object()))  # type: ignore[arg-type]


class CatalogueParseTests(unittest.TestCase):
    """_get_all_series decides what a new-only scrape treats as new."""

    def setUp(self):
        self.catalogue = _parse_catalogue(CATALOGUE)
        self.links = [s["link"] for s in self.catalogue]

    def test_two_series_sharing_a_title_are_both_listed(self):
        self.assertIn("/anime/stream/waldern", self.links)
        self.assertIn("/anime/stream/wldern", self.links)

    def test_one_series_spelled_two_ways_is_listed_once(self):
        self.assertEqual([link for link in self.links if "25" in link], ["/anime/stream/25%20Years%20of%20You"])

    def test_only_the_catalogue_list_is_read_and_season_pages_are_not_series(self):
        self.assertEqual(len(self.catalogue), 3)
        self.assertNotIn("/anime/stream/from-the-menu", self.links)

    def test_alternative_titles_come_from_the_search_attribute(self):
        waldern = next(s for s in self.catalogue if s["link"] == "/anime/stream/waldern")
        self.assertEqual(waldern.get("alt_titles"), ["The Missing Girl"])

    def test_a_logged_out_page_is_refused_rather_than_read_as_empty(self):
        with self.assertRaises(RuntimeError):
            _parse_catalogue(CATALOGUE.replace('<div class="avatar"><a href="/user/profil/me">me</a></div>', ""))


class AccountScopeVanishedTests(unittest.TestCase):
    """Account scrapes only report; deleting is for full catalogue scopes."""

    def setUp(self):
        self.old_data = im._key_series(
            [
                series("Both", slug="both", subscribed=True, watchlist=True),
                series("Watch", slug="watch", subscribed=False, watchlist=True),
                series("Sub", slug="sub", subscribed=True, watchlist=False),
                series("Neither", slug="neither", subscribed=False, watchlist=False),
            ]
        )
        self.path = write_index(list(self.old_data.values()))

    def _report(self, scope):
        no_prompts = mock.patch("builtins.input", side_effect=AssertionError("account scopes must not prompt"))
        with no_prompts, captured_output():
            kept = im.show_vanished_series(self.old_data, set(), scope, index_file=self.path)
        return sorted(title for title, _reason in kept)

    def test_each_scope_reports_only_what_it_covers(self):
        self.assertEqual(self._report("watchlist"), ["Both", "Watch"])
        self.assertEqual(self._report("subscribed"), ["Both", "Sub"])
        self.assertEqual(self._report("both"), ["Both", "Sub", "Watch"])

    def test_nothing_is_deleted(self):
        for scope in ("watchlist", "subscribed", "both"):
            self._report(scope)
        self.assertEqual(_titles_on_disk(self.path), ["Both", "Neither", "Sub", "Watch"])


ALLOW_ALL = dict.fromkeys(ALL_GATES, True)


class MergeGuardTests(unittest.TestCase):
    """Values a failed scrape leaves behind must never overwrite stored ones."""

    def test_a_new_series_with_unknown_subscription_state_is_not_added(self):
        new = series("New", slug="new", subscribed=None, watchlist=False)
        merged = im._build_merged_data({}, {"New": new}, ALLOW_ALL)
        self.assertEqual(merged, {})

    def test_unknown_state_on_a_known_series_keeps_the_stored_value(self):
        old = series("Known", slug="known", subscribed=True, watchlist=True)
        new = series("Known", slug="known", subscribed=None, watchlist=None)
        merged = im._build_merged_data({"Known": old}, {"Known": new}, ALLOW_ALL)
        self.assertTrue(merged["Known"]["subscribed"])
        self.assertTrue(merged["Known"]["watchlist"])

    def test_an_unsafe_url_is_not_stored_over_a_good_one(self):
        old = series("Known", slug="known")
        new = series("Known", slug="known")
        new["url"] = "javascript:alert(1)"
        merged = im._build_merged_data({"Known": old}, {"Known": new}, ALLOW_ALL)
        self.assertEqual(merged["Known"]["url"], old["url"])

    def test_an_episode_with_unknown_watch_state_is_dropped_from_a_new_season(self):
        old = series("Known", slug="known", seasons=1)
        new = series("Known", slug="known", seasons=2)
        new["seasons"][1]["episodes"][0]["watched"] = None
        merged = im._build_merged_data({"Known": old}, {"Known": new}, ALLOW_ALL)
        second = merged["Known"]["seasons"][1]
        self.assertEqual(len(second["episodes"]), 11)
        self.assertEqual(second["total_episodes"], 11)


if __name__ == "__main__":
    unittest.main()
