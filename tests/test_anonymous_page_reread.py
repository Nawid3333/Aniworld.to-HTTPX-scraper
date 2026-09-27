"""A page served logged out is re-read before the session is blamed.

Captured on bs.to under load (#4): 25 of 24,975 pages came back as a normal
200 of the right page, rendered anonymous, in bursts at single instants --
while the session itself stayed valid (21,470 logged-in pages followed with no
re-login). Every such page used to cost a real login POST into the live
session, and once _MAX_RELOGINS was spent it failed its series outright.

Now a flickered page is read once more without logging in, and a login is
only sent when the session really reads as logged out. The invariant that
matters most is pinned too: a page without the login marker never becomes
data -- a persistently anonymous page still fails its series.
"""

from __future__ import annotations

import asyncio
import unittest
from unittest import mock

import src.scraper as sc
from tests import test_season_parse_once as site

SCRAPER_CLS = sc.AniWorldScraper
ANON_SERIES_HTML = site.SERIES_HTML.replace(site.LOGGED_IN, "")


class _Response:
    def __init__(self, text: str) -> None:
        self.text = text
        self.status_code = 200
        self.headers: dict = {}


class _FlickerClient:
    """Serves each URL from a queue of bodies (the last one repeats), counting requests.

    Any URL that is neither the series page nor a season page is the page a
    login is verified against; `session_alive` decides how it reads.
    """

    def __init__(self, series: list[str], seasons: dict[str, list[str]], session_alive: bool = True) -> None:
        self.series = series
        self.seasons = seasons
        self.session_alive = session_alive
        self.requests: dict[str, int] = {}

    async def get(self, url, **_kwargs):
        self.requests[url] = self.requests.get(url, 0) + 1
        if url == site.SERIES_URL:
            queue = self.series
        else:
            key = url.rstrip("/").rsplit("/", 1)[1].replace("staffel-", "")
            if key not in self.seasons:
                return _Response(site.SERIES_HTML if self.session_alive else ANON_SERIES_HTML)
            queue = self.seasons[key]
        body = queue.pop(0) if len(queue) > 1 else queue[0]
        return _Response(body)

    def season_requests(self, key: str) -> int:
        return sum(n for url, n in self.requests.items() if url.rstrip("/").endswith(key))


def _scrape(client):
    scraper = SCRAPER_CLS()
    info = {"url": site.SERIES_URL, "link": site.SERIES_URL, "title": "Test Series"}
    info["link"] = info["url"][len(site.BASE) :]
    login = mock.AsyncMock()
    with mock.patch.object(SCRAPER_CLS, "_login_client", login):
        result = asyncio.run(scraper._scrape_one_series(client, info))  # type: ignore[arg-type]
    return result, login


class TestAFlickerIsReReadNotLoggedInto(unittest.TestCase):
    def test_an_anonymous_series_page_is_re_read_without_a_login(self):
        client = _FlickerClient(
            series=[ANON_SERIES_HTML, site.SERIES_HTML],
            seasons={"1": [site.season_html(True)], "2": [site.season_html(False)]},
        )
        result, login = _scrape(client)
        self.assertFalse(result.get("_error"), result)
        self.assertEqual(login.await_count, 0, "a flicker must not cost a login POST")
        self.assertEqual(result["watched_episodes"], 1)

    def test_only_the_anonymous_season_page_is_re_read(self):
        client = _FlickerClient(
            series=[site.SERIES_HTML],
            seasons={
                "1": [site.season_html(True, logged_in=False), site.season_html(True)],
                "2": [site.season_html(False)],
            },
        )
        result, login = _scrape(client)
        self.assertFalse(result.get("_error"), result)
        self.assertEqual(login.await_count, 0)
        self.assertEqual(result["watched_episodes"], 1, "the re-read, logged-in page is the one stored")
        self.assertEqual(client.season_requests("1"), 2)
        self.assertEqual(client.season_requests("2"), 1, "a page that was fine is not fetched again")

    def test_a_live_session_is_confirmed_instead_of_logged_into(self):
        # Anonymous on both reads, but the session itself still reads as
        # logged in: no login POST, and the page still fails its series.
        client = _FlickerClient(
            series=[site.SERIES_HTML],
            seasons={"1": [site.season_html(True, logged_in=False)], "2": [site.season_html(False)]},
            session_alive=True,
        )
        result, login = _scrape(client)
        self.assertEqual(login.await_count, 0, "no login POST into a live session")
        self.assertTrue(result.get("_error"), "an anonymous page must never become data")
        self.assertIn("not logged in", result["_error_reason"])


class TestARealExpiryStillLogsIn(unittest.TestCase):
    def test_a_dead_session_gets_one_login_and_then_the_page_is_used(self):
        client = _FlickerClient(
            series=[ANON_SERIES_HTML, ANON_SERIES_HTML, site.SERIES_HTML],
            seasons={"1": [site.season_html(True)], "2": [site.season_html(False)]},
            session_alive=False,
        )
        result, login = _scrape(client)
        self.assertEqual(login.await_count, 1)
        self.assertFalse(result.get("_error"), result)

    def test_a_persistently_anonymous_page_still_fails_its_series(self):
        client = _FlickerClient(
            series=[ANON_SERIES_HTML],
            seasons={"1": [site.season_html(True)], "2": [site.season_html(False)]},
            session_alive=False,
        )
        result, login = _scrape(client)
        self.assertEqual(login.await_count, 1, "today's behaviour: one re-login, then give up")
        self.assertTrue(result.get("_error"))
        self.assertIn("not logged in", result["_error_reason"])


class TestTheLoginCapCountsOnlyRealLogins(unittest.TestCase):
    def test_confirming_a_live_session_does_not_use_up_the_cap(self):
        scraper = SCRAPER_CLS()
        client = _FlickerClient(series=[site.SERIES_HTML], seasons={}, session_alive=True)
        with mock.patch.object(SCRAPER_CLS, "_login_client", mock.AsyncMock()) as login:
            for _ in range(sc._MAX_RELOGINS + 3):
                self.assertTrue(asyncio.run(scraper._relogin_shared_client(client)))
        self.assertEqual(login.await_count, 0)
        self.assertEqual(scraper._relogin_count, 0)

    def test_a_dead_session_past_the_cap_stops_being_re_checked(self):
        scraper = SCRAPER_CLS()
        client = _FlickerClient(series=[site.SERIES_HTML], seasons={}, session_alive=False)
        with mock.patch.object(SCRAPER_CLS, "_login_client", mock.AsyncMock()) as login:
            for _ in range(sc._MAX_RELOGINS):
                asyncio.run(scraper._relogin_shared_client(client))
            self.assertFalse(asyncio.run(scraper._relogin_shared_client(client)))
            checks = sum(client.requests.values())
            self.assertFalse(asyncio.run(scraper._relogin_shared_client(client)))
        self.assertEqual(login.await_count, sc._MAX_RELOGINS)
        self.assertEqual(sum(client.requests.values()), checks, "a session known dead is not fetched again")


if __name__ == "__main__":
    unittest.main()
