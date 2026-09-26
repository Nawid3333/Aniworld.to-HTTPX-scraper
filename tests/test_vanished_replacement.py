"""The vanished-series table's replacement flow: pairing, comparison, swap.

A series the site moved to a new URL used to leave two answers: keep the dead
entry, or delete it and let the new-series prompt add the new one, with
nothing tying the two together and nothing checking they were the same show.
The case that exposed it: "Robin Hood no Daibouken" became "Robin Hood no
Daibouken | The Great Adventures of Robin Hood" on bs.to, scored 0.70 against
a 0.75 pairing floor, and was shown with no counterpart at all.

These tests pin what replaced that: candidates limited to series the index
does not hold, pairing on episode titles as well as names, a comparison that
flags every difference between the index and the site, and a swap that lands
in one write -- so the run's save neither adds the series a second time nor
undoes a fix the user made on the site at the prompt.

They also pin that a catalogue stub -- title, link and url, never scraped --
cannot stand in for a series. bs.to's new-only run appended one after each
flagged rename it had already scraped; keyed by title the stub won, so
"Alpensaga" (six episodes) showed as 0/0 and would have been saved empty.
"""

from __future__ import annotations

import asyncio
import json
from unittest import mock

from src import index_manager as im
from src.scraper import AniWorldScraper
from tests._support import captured_output, episode, scripted_input, season, series, write_index

# The one line that differs between the three sibling copies of this file.
SCRAPER_CLS = AniWorldScraper

TITLES = ("Marian", "Geheimnisvoller Sherwood Forrest", "Die Krönung", "Der König der Wälder")


def _show(title, slug, *, watched=0, titles=TITLES, more_seasons=(), **extra):
    """A one-season series whose episodes carry *titles*, the first *watched* seen."""
    eps = [episode(n, watched=n <= watched, title=t) for n, t in enumerate(titles, 1)]
    return series(title, slug=slug, seasons=[season(1, eps=eps), *more_seasons], **extra)


OLD = _show("Robin Hood no Daibouken", "Robin-Hood-no-Daibouken", watched=4)
NEW = _show(
    "Robin Hood no Daibouken | The Great Adventures of Robin Hood",
    "Robin-Hood-no-Daibouken-The-Great-Adventures-of-Robin-Hood",
    watched=4,
)


def _vanished(*entries):
    return [(e["title"], "not found", e["url"]) for e in entries]


def _keyed(*entries):
    return {e["title"]: e for e in entries}


def _stub(entry):
    """The catalogue's listing of a series: no seasons, never scraped."""
    return {key: entry[key] for key in ("title", "link", "url")}


class FakeScraper:
    """Answers fetch_series from a queue and records every URL it was asked for."""

    def __init__(self, *results):
        self.results = list(results)
        self.fetched = []

    async def fetch_series(self, url):
        self.fetched.append(url)
        return self.results.pop(0)

    def normalize_to_series_url(self, url):
        return url


class TestPairing:
    def test_a_pipe_separated_rename_pairs_exactly(self):
        matched = im._match_vanished_to_new(_vanished(OLD), _keyed(NEW), _keyed(OLD))
        assert matched[0][2:] == (NEW["title"], NEW["url"], "exact")

    def test_matching_episode_titles_pair_a_rename_the_title_misses(self):
        old = _show("Haus des Geldes", "haus-des-geldes")
        new = _show("Money Heist", "money-heist")
        assert im._score_match(old["title"], old["url"], new["title"], new["url"]) < 0.75
        matched = im._match_vanished_to_new(_vanished(old), _keyed(new), _keyed(old))
        assert matched[0][2] == "Money Heist" and matched[0][4] == "episodes"

    def test_without_the_index_entry_pairing_falls_back_to_titles(self):
        old = _show("Haus des Geldes", "haus-des-geldes")
        new = _show("Money Heist", "money-heist")
        assert im._match_vanished_to_new(_vanished(old), _keyed(new))[0][2] is None

    def test_generic_episode_titles_are_no_evidence(self):
        generic = ("Folge 1", "Folge 2", "Episode 3", "4")
        old = _show("Haus des Geldes", "haus-des-geldes", titles=generic)
        new = _show("Money Heist", "money-heist", titles=generic)
        assert im._match_vanished_to_new(_vanished(old), _keyed(new), _keyed(old))[0][2] is None

    def test_too_few_titles_are_no_evidence(self):
        old = _show("Haus des Geldes", "haus-des-geldes", titles=TITLES[:2])
        new = _show("Money Heist", "money-heist", titles=TITLES[:2])
        assert im._match_vanished_to_new(_vanished(old), _keyed(new), _keyed(old))[0][2] is None

    def test_episode_evidence_outranks_a_similar_name(self):
        old = _show("Money Heist", "money-heist")
        lookalike = _show("Money Heist: Korea", "money-heist-korea", titles=("A", "B", "C", "D"))
        same_show = _show("Haus des Geldes", "haus-des-geldes")
        matched = im._match_vanished_to_new(_vanished(old), _keyed(lookalike, same_show), _keyed(old))
        assert matched[0][2] == "Haus des Geldes"

    def test_series_the_index_already_holds_are_not_candidates(self):
        """A full scrape hands back every series it read, not only new ones."""
        indexed = _show("Robin Hood (1997)", "Robin-Hood-1997", titles=("W", "X", "Y", "Z"))
        candidates = im._rename_candidates(_keyed(indexed, NEW), _keyed(OLD, indexed))
        assert list(candidates) == [NEW["title"]]

    def test_the_table_never_suggests_an_indexed_series(self):
        duplicate = _show("Robin Hood Duplicate", "robin-hood-duplicate")
        with scripted_input("k") as asked, captured_output():
            im._prompt_vanished_table(_vanished(OLD), _keyed(duplicate), _keyed(OLD, duplicate))
        assert "s=swap" not in asked[0], "no new entry may be offered to swap to"

    def test_a_failed_scrape_placeholder_is_not_a_candidate(self):
        placeholder = dict(NEW, seasons=[], _error=True, _error_reason="timeout")
        assert im._rename_candidates(_keyed(placeholder), _keyed(OLD)) == {}

    def test_a_catalogue_stub_is_not_a_candidate(self):
        assert im._rename_candidates(_keyed(_stub(NEW)), _keyed(OLD)) == {}


class TestUnscrapedEntries:
    def test_only_an_entry_with_a_seasons_list_is_a_scrape_result(self):
        assert im._is_scrape_result(NEW)
        assert im._is_scrape_result(dict(NEW, seasons=[])), "a series whose seasons are all ignored is still scraped"
        assert not im._is_scrape_result(_stub(NEW))
        assert not im._is_scrape_result(dict(NEW, _error=True))

    def test_the_save_never_adds_a_catalogue_stub(self):
        listed_only = _show("Listed Only", "listed-only")
        path = write_index([OLD])
        with scripted_input(default="y"), captured_output() as out:
            im.confirm_and_save_changes(
                [NEW, _stub(NEW), _stub(listed_only)], "data", index_manager=im.IndexManager(path)
            )
        saved = {e["title"]: e for e in im.IndexManager(path).series_index.values()}
        assert saved[NEW["title"]]["total_episodes"] == 4, "the scraped copy is what gets saved"
        assert "Listed Only" not in saved, "a series that was only listed is not added"
        assert "listed but never scraped" in out.getvalue()


class TestDifferences:
    def test_an_identical_replacement_shows_nothing_to_fix(self):
        lines, differences = im._replacement_differences(OLD, NEW)
        assert differences == 0
        assert all(line.startswith("✓") for line in lines)

    def test_every_kind_of_difference_is_flagged(self):
        old = _show("Old", "old", watched=4, more_seasons=[season(2, episodes=3)])
        eps = [
            episode(1, watched=True, title=TITLES[0]),
            episode(2, watched=False, title=TITLES[1]),
            episode(3, watched=True, title="Ein anderer Titel"),
            episode(5, watched=False, title="Neu"),
        ]
        new = series("New", slug="new", seasons=[season(1, eps=eps), season("Specials", episodes=1)])
        lines, differences = im._replacement_differences(old, new)
        text = "\n".join(lines)
        assert "missing on the site: episode(s) 4" in text
        assert "only on the site: episode(s) 5" in text
        assert "titles differ: episode(s) 3" in text
        assert "watched in the index, unwatched on the site: episode(s) 2" in text
        assert "Season 2: only in the index" in text
        assert "Specials: only on the site" in text
        assert differences == 6

    def test_watched_on_the_site_only_is_flagged_too(self):
        lines, differences = im._replacement_differences(_show("Old", "old"), _show("New", "new", watched=2))
        assert differences == 1
        assert "unwatched in the index, watched on the site: episode(s) 1-2" in "\n".join(lines)

    def test_sub_and_watchlist_flags_are_compared_where_present(self):
        old = _show("Old", "old", subscribed=True, watchlist=False)
        new = _show("New", "new", subscribed=False, watchlist=False)
        lines, differences = im._replacement_differences(old, new)
        assert differences == 1
        assert any(line.startswith("⚠ Sub") for line in lines)

    def test_episode_numbers_are_shown_as_ranges(self):
        assert im._episode_ranges([9, 1, 2, 3, 7, 10]) == "1-3, 7, 9-10"


class TestReplacePrompt:
    def _run(self, *answers, vanished=(OLD,), new=(NEW,), index=(), scraper=None):
        replacements = []
        with scripted_input(*answers, default="k") as asked, captured_output() as out:
            deleted = im._prompt_vanished_table(
                _vanished(*vanished),
                _keyed(*new),
                _keyed(*vanished, *index),
                scraper=scraper,
                replacements=replacements,
            )
        self.asked, self.out = asked, out.getvalue()
        assert deleted == [], "a replacement is not a delete"
        return replacements

    def test_swap_compares_with_the_shown_entry_and_y_replaces(self):
        assert self._run("s", "y") == [(OLD["title"], NEW)]
        assert "s=swap-to-new" in self.asked[0]
        assert len(self.asked) == 2, "swap asks for no URL, only for the y/n"
        assert "The site matches the index" in self.out

    def test_an_exact_match_still_needs_an_explicit_yes(self):
        assert self._run("s", "", "k") == []

    def test_swap_is_not_offered_without_a_new_entry(self):
        assert self._run("s", "k", new=()) == []
        assert "s=swap" not in self.asked[0]
        assert "shows no new entry to swap to" in self.out

    def test_link_asks_for_a_url_and_enter_cancels(self):
        assert self._run("l", "", "k") == []
        assert "Paste the replacement's URL" in self.asked[1]
        assert "Cancelled" in self.out

    def test_a_url_outside_the_run_is_scraped_live(self):
        scraper = FakeScraper(NEW)
        assert self._run("l", NEW["url"], "y", new=(), scraper=scraper) == [(OLD["title"], NEW)]
        assert scraper.fetched == [NEW["url"]]

    def test_rescrape_reads_the_site_again_after_a_fix(self):
        unwatched = _show(NEW["title"], "Robin-Hood-no-Daibouken-The-Great-Adventures-of-Robin-Hood")
        scraper = FakeScraper(NEW)
        result = self._run("s", "r", "y", new=(unwatched,), scraper=scraper)
        assert result == [(OLD["title"], NEW)], "the re-scraped state is what replaces"
        assert "watched in the index, unwatched on the site: episode(s) 1-4" in self.out
        assert self.out.index("unwatched on the site") < self.out.index("The site matches the index")

    def test_open_shows_only_the_new_link(self):
        with mock.patch.object(im, "_open_urls_for_comparison", autospec=True) as opened:
            assert self._run("s", "o", "n", "k") == []
        opened.assert_called_once_with("", NEW["url"])

    def test_the_entrys_own_url_is_rejected(self):
        scraper = FakeScraper()
        assert self._run("l", OLD["url"], "k", new=(), scraper=scraper) == []
        assert scraper.fetched == []
        assert "this entry's own URL" in self.out

    def test_one_series_cannot_replace_two_entries(self):
        other = _show("Robin Hood (Remaster)", "Robin-Hood-Remaster", titles=("A", "B", "C", "D"))
        result = self._run("s", "y", "l", NEW["url"], "k", vanished=(OLD, other))
        assert result == [(OLD["title"], NEW)]
        assert "Already chosen as the replacement" in self.out

    def test_a_failed_live_scrape_replaces_nothing(self):
        scraper = FakeScraper({"_error": True, "_error_reason": "no seasons found"})
        assert self._run("l", NEW["url"], "k", new=(), scraper=scraper) == []
        assert "no seasons found" in self.out

    def test_a_series_the_run_failed_to_read_is_read_live(self):
        placeholder = dict(NEW, seasons=[], _error=True, _error_reason="timeout")
        scraper = FakeScraper(NEW)
        assert self._run("l", NEW["url"], "y", new=(placeholder,), scraper=scraper) == [(OLD["title"], NEW)]
        assert scraper.fetched == [NEW["url"]]

    def test_closed_stdin_replaces_nothing(self):
        for action in ("s", "l"):
            replacements = []
            with mock.patch("builtins.input", side_effect=[action, EOFError, EOFError]), captured_output():
                im._prompt_vanished_table(_vanished(OLD), _keyed(NEW), _keyed(OLD), replacements=replacements)
            assert replacements == [], action


class TestReplaceInIndex:
    def test_the_swap_lands_in_one_write_in_the_old_entrys_place(self):
        before, after = _show("Before", "before"), _show("After", "after")
        old = dict(OLD, added_date="2026-07-27T19:18:56")
        path = write_index([before, old, after])
        fetched = dict(NEW, _verified_reachable=True, total_episodes=999)
        assert im.replace_series_in_index(path, [(old, fetched)]) == (1, 1)
        with open(path, encoding="utf-8") as f:
            index = json.load(f)
        assert [e["title"] for e in index] == ["Before", NEW["title"], "After"]
        swapped = index[1]
        assert swapped["added_date"] == old["added_date"], "the first-indexed date carries over"
        assert swapped["total_episodes"] == 4, "counters come from the episode lists"
        assert "_verified_reachable" not in swapped

    def test_an_already_indexed_replacement_only_removes_the_old_entry(self):
        existing = dict(NEW, added_date="2026-09-20T00:00:00")
        path = write_index([OLD, existing])
        assert im.replace_series_in_index(path, [(OLD, NEW)]) == (1, 0)
        with open(path, encoding="utf-8") as f:
            assert json.load(f) == [existing]


class TestShowVanishedSeries:
    def _show_vanished(self, run_data, *answers, scraper=None, index=(OLD,)):
        path = write_index(list(index))
        idx = im.IndexManager(path)
        discovered = {im._extract_slug(e) for e in run_data}
        with scripted_input(*answers, default="k"), captured_output() as out:
            im.show_vanished_series(
                idx.series_index, discovered, "new_only", index_file=path, new_data=run_data, scraper=scraper
            )
        self.out = out.getvalue()
        return path

    def test_replacing_leaves_one_entry_and_the_save_adds_nothing(self):
        run_data = [NEW]
        path = self._show_vanished(run_data, "s", "y")
        idx = im.IndexManager(path)
        assert [im._extract_slug(e) for e in idx.series_index.values()] == [im._extract_slug(NEW)]
        with scripted_input(default="n") as asked, captured_output() as out:
            im.confirm_and_save_changes(run_data, "data", index_manager=idx)
        # Nothing to add, nothing to flip: the replacement is not offered as a
        # new series a second time, and the save has no question to ask.
        assert "already up to date" in out.getvalue()
        assert asked == []

    def test_a_rescraped_replacement_overwrites_the_runs_copy(self):
        stale = _show(NEW["title"], "Robin-Hood-no-Daibouken-The-Great-Adventures-of-Robin-Hood")
        run_data = [stale]
        # "n" declines the candidate verification asked before the table.
        self._show_vanished(run_data, "n", "s", "r", "y", scraper=FakeScraper(NEW))
        assert run_data[0] is NEW, "the save must diff what the site shows now"

    def test_a_full_scrape_does_not_list_indexed_series_as_new(self):
        indexed = [_show(f"Filler {i}", f"filler-{i}", titles=(f"a{i}", f"b{i}", f"c{i}")) for i in range(3)]
        unrelated = _show("Something Else", "something-else", titles=("p", "q", "r"))
        self._show_vanished([*indexed, unrelated], "k", index=(OLD, *indexed))
        assert "+ 1 new series not linked" in self.out
        assert "Filler" not in self.out

    def test_an_unscraped_copy_never_shadows_the_scraped_one(self):
        path = self._show_vanished([NEW, _stub(NEW)], "s", "y")
        assert "[0]: 0/0" not in self.out, "the row must show the scraped entry, not the stub"
        [entry] = im.IndexManager(path).series_index.values()
        assert entry["total_episodes"] == 4, "the swap must write the scraped entry"


class TestFetchSeries:
    def test_one_url_is_scraped_on_its_own_client_which_is_closed(self):
        scraper = SCRAPER_CLS()
        client = mock.AsyncMock()
        with (
            mock.patch.object(scraper, "_create_logged_in_client", mock.AsyncMock(return_value=client)),
            mock.patch.object(scraper, "_scrape_one_series", mock.AsyncMock(return_value=NEW)) as scrape,
        ):
            assert asyncio.run(scraper.fetch_series(NEW["url"])) is NEW
        info = scrape.call_args[0][1]
        assert info["url"].endswith("Robin-Hood-no-Daibouken-The-Great-Adventures-of-Robin-Hood")
        client.aclose.assert_awaited_once()
