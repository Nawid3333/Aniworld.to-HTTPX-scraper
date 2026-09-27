"""Every fetched season page is parsed exactly once.

The logged-out screen in _scrape_one_series used to run the full season
parse on every page, and the per-season loop right after it parsed every
page again. Over HTTP/1.1 a run is bound by one CPU core, and that second
parse was ~4.6 ms per season page, for no information.
These tests count the parses so the double read cannot creep back, and pin
the behaviour the screen must keep: a re-login refetches and re-parses the
fresh pages, and a failed fetch is still reported as a fetch failure.
"""

from __future__ import annotations

import asyncio
import unittest
from unittest import mock

import src.scraper as sc

BASE = "https://aniworld.to"
SERIES_URL = f"{BASE}/anime/stream/test-series"
SEASONS = ("1", "2")

# The profile link is what proves a page was served to a logged-in session,
# and its name is the account name learned from the series page.
LOGGED_IN = '<div class="avatar"><a href="/user/profil/1">Me</a></div>'
LINKS = "".join(f'<li><a href="/anime/stream/test-series/staffel-{n}">{n}</a></li>' for n in SEASONS)
SERIES_HTML = f"""
<html><body>
{LOGGED_IN}
<h1 class="fw-bold">Test Series</h1>
<div id="stream"><ul>{LINKS}</ul></div>
</body></html>
"""


def season_html(watched: bool, logged_in: bool = True) -> str:
    row_class = ' class="seen"' if watched else ""
    return (
        f"<html><body>{LOGGED_IN if logged_in else ''}"
        f'<table class="seasonEpisodesList"><tbody><tr data-episode-id="1"{row_class}>'
        '<meta itemprop="episodeNumber" content="1">'
        '<td class="seasonEpisodeTitle"><a><strong>Pilot</strong></a></td>'
        "</tr></tbody></table></body></html>"
    )


class _Response:
    def __init__(self, text: str) -> None:
        self.text = text
        self.status_code = 200
        self.headers: dict = {}


class _Client:
    """Serves the series page and one body per season; a body may be an exception."""

    def __init__(self, seasons: dict) -> None:
        self.seasons = seasons

    async def get(self, url, **_kwargs):
        if url == SERIES_URL:
            return _Response(SERIES_HTML)
        body = self.seasons[url.rsplit("-", 1)[1]]
        if isinstance(body, BaseException):
            raise body
        return _Response(body)


def _run(client, relogin=None):
    scraper = sc.AniWorldScraper()
    info = {"url": SERIES_URL, "link": "/anime/stream/test-series", "title": "Test Series"}
    counted = mock.Mock(wraps=sc.parse_season_page)
    with mock.patch.object(sc, "parse_season_page", counted):
        if relogin is not None:
            scraper._relogin_shared_client = relogin  # type: ignore[method-assign]
        result = asyncio.run(scraper._scrape_one_series(client, info))  # type: ignore[arg-type]
    return result, counted.call_count


class TestSeasonPagesParsedOnce(unittest.TestCase):
    def test_each_season_page_is_parsed_once(self):
        client = _Client({"1": season_html(True), "2": season_html(False)})
        result, parses = _run(client)
        self.assertFalse(result.get("_error"), result)
        self.assertEqual(parses, len(SEASONS))
        self.assertEqual(result["watched_episodes"], 1)
        self.assertEqual(result["total_episodes"], 2)

    def test_a_relogin_parses_the_refetched_pages_and_uses_them(self):
        client = _Client({"1": season_html(False, logged_in=False), "2": season_html(False)})

        async def relogin(_client):
            # The session is back: this time season 1 is served logged in,
            # and watched, so the result shows which read was used.
            client.seasons["1"] = season_html(True)
            return True

        result, parses = _run(client, relogin=relogin)
        self.assertFalse(result.get("_error"), result)
        # Every page once, the anonymous one once more on the no-login re-read
        # (still anonymous here), then every page once after the re-login.
        self.assertEqual(parses, 2 * len(SEASONS) + 1)
        self.assertEqual(result["watched_episodes"], 1, "the fresh pages must be the ones stored")

    def test_a_failed_relogin_does_not_parse_again(self):
        client = _Client({"1": season_html(True, logged_in=False), "2": season_html(False)})
        result, parses = _run(client, relogin=mock.AsyncMock(return_value=False))
        self.assertTrue(result.get("_error"))
        self.assertIn("not logged in", result["_error_reason"])
        # Every page once plus the anonymous one's re-read; nothing after the
        # failed re-login.
        self.assertEqual(parses, len(SEASONS) + 1)

    def test_a_failed_fetch_is_reported_as_one_and_not_parsed(self):
        client = _Client({"1": RuntimeError("connection dropped"), "2": season_html(True)})
        result, parses = _run(client)
        self.assertTrue(result.get("_error"))
        self.assertIn("season 1 fetch failed", result["_error_reason"])
        self.assertEqual(parses, 1, "only the page that arrived is parsed")


if __name__ == "__main__":
    unittest.main()
