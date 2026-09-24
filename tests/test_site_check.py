"""The monthly live-site check, run against a fake site.

The check can only earn trust by being right in both directions: silent on a
healthy site, loud on each kind of change the scraper depends on, and never
calling a blocked runner a broken site. Nothing here touches the network.
"""

from __future__ import annotations

import asyncio
import os
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest import mock

import httpx

from tests import fixture_spec, site_check
from tests.site_check import FAIL, PASS, SKIPPED, UNREACHABLE

# ── This site ───────────────────────────────────────────────────────────────
# Everything down to the next rule describes aniworld.to. The rest of this
# file is the same in all three scrapers.
HOST = "https://aniworld.to"
MIRROR = "https://aniworld.cc"
LOGIN_PATH = "/login"
SERIES_PATH = fixture_spec.SERIES_PATH.format(slug=fixture_spec.PROBE_SLUGS[0])
ACCOUNT_PATHS = ("/account/subscribed", "/account/watchlist")
ACCOUNT_NAME = "SecretAccountName"

# The logged-in chrome: the avatar's profile link _is_logged_in looks for,
# which carries the account name the report must never repeat.
CHROME = f'<div class="avatar"><a href="/user/profil/{ACCOUNT_NAME}">{ACCOUNT_NAME}</a></div>'

LOGIN = """<html><body><form action="/login" method="post">
<input type="email" name="email"><input type="password" name="password">
<input type="checkbox" name="autoLogin"></form></body></html>"""


def season_path(series_path: str) -> str:
    return f"{series_path}/staffel-1"


def films_path(series_path: str) -> str:
    """The season the check should try last: films and specials."""
    return f"{series_path}/filme"


def series_page(series_path: str) -> str:
    return f"""<html><head><title>One Piece</title></head><body>{{chrome}}
<h1 itemprop="name"><span>One Piece</span></h1>
<div id="stream"><ul>
<li><a href="{films_path(series_path)}">Filme</a></li>
<li><a href="{season_path(series_path)}">1</a></li>
</ul></div>{{buttons}}</body></html>"""


SERIES_WITHOUT_SEASONS = (
    series_page(SERIES_PATH).replace('id="stream"', 'id="stream-v2"').replace("staffel-", "season/")
)

BUTTONS = """<div class="add-series" data-series-favourite="0" data-series-watchlist="1"></div>
<ul><li class="setFavourite"></li><li class="setWatchlist true"></li></ul>"""

SEASON = """<html><body>{chrome}<table class="seasonEpisodesList"><tbody>
<tr data-episode-id="11"><td><meta itemprop="episodeNumber" content="1"></td>
<td class="seasonEpisodeTitle"><a href="#"><strong>Ich bin Ruffy</strong><span>I'm Luffy</span></a></td></tr>
<tr data-episode-id="12"><td><meta itemprop="episodeNumber" content="2"></td>
<td class="seasonEpisodeTitle"><a href="#"><strong>Der Schwertkämpfer</strong><span>The Swordsman</span></a></td></tr>
</tbody></table></body></html>"""

SEASON_WITHOUT_TITLES = SEASON.replace("seasonEpisodeTitle", "episodeName")

CATALOGUE = """<html><body>{chrome}<div id="seriesContainer"><ul>
<li><a href="/anime/stream/one-piece">One Piece</a></li>
<li><a href="/anime/stream/naruto">Naruto</a></li>
<li><a href="/anime/stream/waldern">Wäldern</a></li>
</ul></div></body></html>"""

# ── The same in every scraper ───────────────────────────────────────────────
SEASON_PATH = season_path(SERIES_PATH)
FILMS_PATH = films_path(SERIES_PATH)
ACCOUNT = "<html><body>{chrome}<p>Keine Serien</p></body></html>"
CHALLENGE = "<html><head><title>Just a moment...</title></head><body>cf-chl</body></html>"


class FakeSite:
    """Serves pages by path and keeps a login session like the real site."""

    def __init__(self, pages: dict | None = None, *, accept_login: bool = True):
        self.pages = {
            LOGIN_PATH: LOGIN,
            "/": "<html><body>{chrome}</body></html>",
            SERIES_PATH: series_page(SERIES_PATH),
            FILMS_PATH: SEASON,
            SEASON_PATH: SEASON,
            fixture_spec.CATALOGUE_PATH: CATALOGUE,
            **dict.fromkeys(ACCOUNT_PATHS, ACCOUNT),
        }
        self.pages.update(pages or {})
        self.accept_login = accept_login
        self.logged_in = False
        self.hosts_down: set[str] = set()
        self.requested: list[str] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        if request.url.host in self.hosts_down:
            raise httpx.ConnectError("connection refused", request=request)
        self.requested.append(request.url.path)
        if request.method == "POST" and request.url.path == LOGIN_PATH:
            self.logged_in = self.accept_login
            return httpx.Response(200, text="")
        page = self.pages.get(request.url.path)
        if page is None:
            return httpx.Response(404, text="<html><head><title>404 Nicht gefunden</title></head></html>")
        status, html = page if isinstance(page, tuple) else (200, page)
        html = html.replace("{chrome}", CHROME if self.logged_in else "")
        html = html.replace("{buttons}", BUTTONS if self.logged_in else "")
        return httpx.Response(status, text=html)


def run(site: FakeSite, *, credentials: bool = False) -> tuple[str | None, dict[str, site_check.Result]]:
    transport = httpx.MockTransport(site)
    real_client = httpx.AsyncClient

    class RoutedClient(real_client):
        def __init__(self, *args, **kwargs):
            kwargs.pop("http2", None)
            kwargs["transport"] = transport
            super().__init__(*args, **kwargs)

    env = dict.fromkeys(fixture_spec.CREDENTIAL_VARS, "x@example.org" if credentials else "")
    with (
        mock.patch.object(httpx, "AsyncClient", RoutedClient),
        mock.patch.dict(os.environ, env),
        mock.patch.object(fixture_spec, "MIN_CATALOGUE", 3),
    ):
        host, results = asyncio.run(site_check.run_checks())
    return host, {r.check: r for r in results}


def statuses(results: dict[str, site_check.Result]) -> dict[str, str]:
    return {name: r.status for name, r in results.items()}


class HealthySiteTests(unittest.TestCase):
    def test_every_public_check_passes_and_the_logged_in_ones_are_skipped(self):
        host, results = run(FakeSite())
        self.assertEqual(host, HOST)
        self.assertEqual(
            statuses(results),
            {"login page": PASS, "series page": PASS, "season page": PASS, "logged-in checks": SKIPPED},
        )
        self.assertEqual(site_check.exit_code(list(results.values())), 0)

    def test_a_numbered_season_is_checked_before_the_films(self):
        site = FakeSite()
        run(site)
        self.assertIn(SEASON_PATH, site.requested)
        self.assertNotIn(FILMS_PATH, site.requested)

    def test_with_credentials_every_logged_in_check_passes(self):
        _host, results = run(FakeSite(), credentials=True)
        self.assertEqual(
            statuses(results),
            {
                "login page": PASS,
                "series page": PASS,
                "season page": PASS,
                "login": PASS,
                "catalogue": PASS,
                "logged-in series page": PASS,
                **{f"account page {path}": PASS for path in ACCOUNT_PATHS},
            },
        )

    def test_the_report_never_carries_the_account_name(self):
        host, results = run(FakeSite(), credentials=True)
        report = site_check.render(host, list(results.values()), date(2026, 10, 3))
        self.assertNotIn(ACCOUNT_NAME, report)
        self.assertNotIn("x@example.org", report)


class LayoutChangeTests(unittest.TestCase):
    """Each change the scraper depends on fails its own check, and only that one."""

    def assert_only_failure(self, results: dict[str, site_check.Result], check: str) -> None:
        failed = [name for name, r in results.items() if r.status == FAIL]
        self.assertEqual(failed, [check])
        self.assertEqual(site_check.exit_code(list(results.values())), 1)

    def test_a_login_page_without_a_password_field(self):
        _host, results = run(FakeSite({LOGIN_PATH: "<html><body><p>Anmelden</p></body></html>"}))
        self.assert_only_failure(results, "login page")

    def test_a_renamed_login_field(self):
        field = fixture_spec.LOGIN_FIELDS[0]
        _host, results = run(FakeSite({LOGIN_PATH: LOGIN.replace(f'name="{field}"', 'name="renamed"')}))
        self.assert_only_failure(results, "login page")
        self.assertIn(field, results["login page"].detail)

    def test_a_series_page_without_season_links(self):
        _host, results = run(FakeSite({SERIES_PATH: SERIES_WITHOUT_SEASONS}))
        self.assert_only_failure(results, "series page")

    def test_a_season_page_without_the_episode_table(self):
        page = "<html><body><div class='episodes-grid'><div>1</div></div></body></html>"
        _host, results = run(FakeSite({SEASON_PATH: page, FILMS_PATH: page}))
        self.assert_only_failure(results, "season page")

    def test_episodes_that_lost_their_titles(self):
        page = SEASON_WITHOUT_TITLES
        _host, results = run(FakeSite({SEASON_PATH: page, FILMS_PATH: page}))
        self.assert_only_failure(results, "season page")

    def test_a_catalogue_far_below_the_floor(self):
        _host, results = run(
            FakeSite({fixture_spec.CATALOGUE_PATH: "<html><body>{chrome}</body></html>"}), credentials=True
        )
        self.assert_only_failure(results, "catalogue")

    def test_a_series_page_without_subscribe_buttons(self):
        if not fixture_spec.HAS_ACCOUNT_BUTTONS:
            self.skipTest("this site has no subscribe/watchlist buttons")
        page = series_page(SERIES_PATH).replace("{buttons}", "")
        _host, results = run(FakeSite({SERIES_PATH: page}), credentials=True)
        self.assert_only_failure(results, "logged-in series page")

    def test_a_series_page_that_drops_the_logged_in_marker(self):
        # Login is verified on another page, so the marker can go from the
        # series and season pages alone -- and with it every watched flag.
        page = series_page(SERIES_PATH).replace("{chrome}", "")
        _host, results = run(FakeSite({SERIES_PATH: page}), credentials=True)
        self.assert_only_failure(results, "logged-in series page")
        self.assertIn("unwatched", results["logged-in series page"].detail)

    def test_an_account_page_that_moved(self):
        if not ACCOUNT_PATHS:
            self.skipTest("this site has no account pages")
        site = FakeSite()
        del site.pages[ACCOUNT_PATHS[-1]]
        _host, results = run(site, credentials=True)
        self.assert_only_failure(results, f"account page {ACCOUNT_PATHS[-1]}")

    def test_a_rejected_login(self):
        _host, results = run(FakeSite(accept_login=False), credentials=True)
        self.assert_only_failure(results, "login")

    def test_every_mirror_answering_404_for_the_login_page(self):
        site = FakeSite()
        del site.pages[LOGIN_PATH]
        host, results = run(site)
        self.assertIsNone(host)
        self.assert_only_failure(results, "login page")


class ProbeSeriesTests(unittest.TestCase):
    def test_a_probe_series_that_has_gone_is_skipped(self):
        site = FakeSite()
        del site.pages[SERIES_PATH]
        second = fixture_spec.SERIES_PATH.format(slug=fixture_spec.PROBE_SLUGS[1])
        site.pages[second] = series_page(second)
        site.pages[season_path(second)] = SEASON
        _host, results = run(site)
        self.assertEqual(results["series page"].status, PASS)
        self.assertTrue(results["series page"].detail.startswith(fixture_spec.PROBE_SLUGS[1]))

    def test_the_home_page_supplies_probes_when_every_fixed_one_has_gone(self):
        site = FakeSite()
        del site.pages[SERIES_PATH]
        other = fixture_spec.SERIES_PATH.format(slug="dark")
        site.pages["/"] = f'<html><body><a href="{other}">Dark</a></body></html>'
        site.pages[other] = series_page(other)
        site.pages[season_path(other)] = SEASON
        _host, results = run(site)
        self.assertEqual(results["series page"].status, PASS)
        self.assertTrue(results["series page"].detail.startswith("dark"))

    def test_no_series_page_anywhere_is_a_failure(self):
        site = FakeSite()
        del site.pages[SERIES_PATH]
        _host, results = run(site)
        self.assertEqual(results["series page"].status, FAIL)


class UnreachableTests(unittest.TestCase):
    """A blocked or down site says nothing about its layout."""

    def test_a_bot_check_on_every_mirror_is_unreachable_not_broken(self):
        site = FakeSite({LOGIN_PATH: (403, CHALLENGE)})
        host, results = run(site)
        self.assertIsNone(host)
        self.assertEqual(statuses(results), {"login page": UNREACHABLE})
        self.assertIn("bot check", results["login page"].detail)
        self.assertEqual(site_check.exit_code(list(results.values())), 2)

    def test_a_down_primary_falls_back_to_the_mirror(self):
        site = FakeSite()
        site.hosts_down.add(HOST.removeprefix("https://"))
        host, results = run(site)
        self.assertEqual(host, MIRROR)
        self.assertEqual(results["season page"].status, PASS)

    def test_a_plain_http_mirror_is_never_used(self):
        # The logged-in checks would send the password over it.
        site = FakeSite()
        site.hosts_down.update(u.removeprefix("https://") for u in site_check.SITE_URLS if u.startswith("https://"))
        host, results = run(site)
        self.assertIsNone(host)
        self.assertEqual(statuses(results), {"login page": UNREACHABLE})

    def test_a_server_error_mid_check_is_unreachable(self):
        _host, results = run(FakeSite({SEASON_PATH: (502, "<html>Bad Gateway</html>")}))
        self.assertEqual(results["season page"].status, UNREACHABLE)
        self.assertEqual(site_check.exit_code(list(results.values())), 2)

    def test_a_failure_outranks_an_unreachable_page(self):
        results = [site_check.Result("a", UNREACHABLE, ""), site_check.Result("b", FAIL, "")]
        self.assertEqual(site_check.exit_code(results), 1)


class UnreachableReasonTests(unittest.TestCase):
    def reason(self, status: int, text: str = "<html></html>", headers: dict | None = None) -> str | None:
        return site_check.unreachable_reason(httpx.Response(status, text=text, headers=headers or {}))

    def test_an_ordinary_page_is_checkable(self):
        self.assertIsNone(self.reason(200))

    def test_a_page_that_mentions_a_captcha_is_still_checkable(self):
        self.assertIsNone(self.reason(200, "<html><title>Login</title><div class='g-recaptcha'>captcha</div></html>"))

    def test_a_challenge_served_with_200_is_not(self):
        self.assertIn("bot-check", self.reason(200, CHALLENGE) or "")

    def test_cloudflares_challenge_header_is_not(self):
        self.assertIn("bot check", self.reason(403, headers={"cf-mitigated": "challenge"}) or "")

    def test_rate_limits_and_server_errors_are_not(self):
        for status in (429, 500, 503):
            with self.subTest(status=status):
                self.assertEqual(self.reason(status), f"HTTP {status}")


class MainTests(unittest.TestCase):
    def test_the_report_file_and_exit_code(self):
        result = site_check.Result("login page", FAIL, "a | b")
        with (
            tempfile.TemporaryDirectory() as tmp,
            mock.patch.object(site_check, "run_checks", mock.AsyncMock(return_value=(HOST, [result]))),
            mock.patch("builtins.print"),
        ):
            report = Path(tmp, "report.md")
            self.assertEqual(site_check.main(["--report", str(report)]), 1)
            text = report.read_text(encoding="utf-8")
        self.assertIn(HOST.removeprefix("https://"), text)
        self.assertIn("a / b", text)  # a pipe would split the table cell

    def test_a_crash_is_reported_as_one_not_as_a_failed_check(self):
        with (
            mock.patch.object(site_check, "run_checks", mock.AsyncMock(side_effect=KeyError("boom"))),
            mock.patch("builtins.print"),
            mock.patch("traceback.print_exc"),
        ):
            self.assertEqual(site_check.main([]), 3)


if __name__ == "__main__":
    unittest.main()
