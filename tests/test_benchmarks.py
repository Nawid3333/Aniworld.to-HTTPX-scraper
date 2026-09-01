"""Timing benchmarks for the paths where cost actually scales.

Skipped unless ``--benchmark`` is passed. See ``tests/bench.py`` for the
harness, the tolerance, and how to re-record the baseline.

What belongs here
-----------------
Work whose cost grows with the size of the index or of a page, and where a
regression would be invisible in a correctness test: the episode parser (run
once per season page, thousands of times per full scrape), the merge (walks
every episode of every series), and the index load/save (a 19 MB file here).

What does not belong here: anything touching the network, anything whose
runtime is dominated by sleeps, and micro-benchmarks of functions called a
handful of times per run -- they add noise and maintenance for no signal.

Every benchmark builds its input once, outside the timed callable, so the
number reflects the operation and not the setup.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest

from src.index_manager import (
    _build_merged_data,
    _detect_episode_count_mismatches,
    detect_changes,
    get_episode_counts,
)
from src.scraper import make_soup, parse_season_html
from tests._support import season, series, write_index

PAGE_DIR = Path(__file__).resolve().parent / "fixtures" / "pages"


def _load_page(stem: str) -> str | None:
    path = PAGE_DIR / f"{stem}.html.gz"
    if not path.exists():
        return None
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return fh.read()


def _largest_season_page() -> str | None:
    """The biggest captured season page, which is the honest parser input.

    Sized by decompressed length rather than by the .gz on disk. The two
    orderings genuinely disagree -- on s.to the largest archive is not the
    largest page -- and what the parser costs depends on the text it is
    handed, not on how well that text happened to compress. Each archive is
    opened once and the winning text kept, so this reads no more than sorting
    on decompressed size would have.
    """
    largest: str | None = None
    for path in PAGE_DIR.glob("season__*.html.gz"):
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            html = fh.read()
        if largest is None or len(html) > len(largest):
            largest = html
    return largest


def _index(series_count: int, *, seasons: int = 4, episodes: int = 24) -> dict:
    """A synthetic index of a realistic shape, keyed by title like the real one."""
    return {
        f"Series {n:04d}": series(
            f"Series {n:04d}",
            seasons=[season(s, episodes=episodes, watched=episodes // 2) for s in range(1, seasons + 1)],
        )
        for n in range(series_count)
    }


# ── parsing ─────────────────────────────────────────────────────────────────


@pytest.mark.benchmark
def test_episode_parser_on_a_real_season_page(bench):
    """Runs once per season page -- thousands of times in a full scrape.

    Benchmarks parse_season_html rather than _parse_episodes: it is the
    HTML-taking entry point in all three sibling repos (bs.to's
    _parse_episodes takes an already-built tree), so the three baselines
    measure the same end-to-end operation and stay comparable.
    """
    html = _largest_season_page()
    if html is None:
        pytest.skip("no captured pages -- run tests/capture_fixtures.py")
    assert parse_season_html(html), "fixture must parse, or the benchmark measures the failure path"
    bench("parse_season_html/real_season_page", lambda: parse_season_html(html))


@pytest.mark.benchmark
def test_soup_build_on_a_real_season_page(bench):
    """The BeautifulSoup path the series pages still use.

    Kept next to the lxml parser above because the gap between the two is the
    whole reason the episode parser bypasses soup.
    """
    html = _largest_season_page()
    if html is None:
        pytest.skip("no captured pages -- run tests/capture_fixtures.py")
    bench("make_soup/real_season_page", lambda: make_soup(html), repeats=3)


# ── merge and change detection ──────────────────────────────────────────────


@pytest.mark.benchmark
def test_merge_of_a_full_scrape(bench):
    """_build_merged_data walks every episode of every series, twice over."""
    old = _index(300)
    new = _index(300)
    allowed = {
        "new_series": True,
        "new_episodes": True,
        "watched": True,
        "unwatched": True,
        "subscribe": True,
        "unsubscribe": True,
        "watchlist_add": True,
        "watchlist_remove": True,
        "title_ger": True,
        "title_eng": True,
        "episode_remove": False,
        "season_remove": False,
    }
    bench("merge/300_series_x_4_seasons", lambda: _build_merged_data(old, new, allowed), repeats=3)


@pytest.mark.benchmark
def test_change_detection_over_a_full_index(bench):
    old = _index(300)
    new = _index(300)
    bench("detect_changes/300_series", lambda: detect_changes(old, new), repeats=3)


@pytest.mark.benchmark
def test_episode_count_mismatch_scan(bench):
    old = _index(300)
    new = _index(300)
    bench("mismatch_scan/300_series", lambda: _detect_episode_count_mismatches(old, new), repeats=3)


@pytest.mark.benchmark
def test_episode_counting_over_a_full_index(bench):
    """Cheap per call, but called for every series on every report and save."""
    entries = list(_index(300).values())
    bench("get_episode_counts/300_series", lambda: [get_episode_counts(e) for e in entries])


# ── index I/O ───────────────────────────────────────────────────────────────


@pytest.mark.benchmark
def test_index_json_round_trip(bench, tmp_path):
    """Load+parse dominates startup on the real 19 MB index."""
    entries = list(_index(300).values())
    path = write_index(entries, tmp_path)

    def load():
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)

    bench("index/json_load_300_series", load, repeats=3)
