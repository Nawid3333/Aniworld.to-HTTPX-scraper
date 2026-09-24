"""Check the live site for changes that would break this scraper.

Run monthly by .github/workflows/site-check.yml, which opens an issue when a
check fails, comments on it while it stays open, and closes it once a run
passes again. Also runnable by hand from the project root:

    python tests/site_check.py [--report FILE]

Every check runs this scraper's own code against today's pages: the mirror
probe's login-page test, the parsers behind fixture_spec.parse_all, and, with
credentials, the real login and catalogue fetch. So a check fails when the
scraper itself would, not merely when the site looks different.

Without credentials only public pages are read: the login form, one series
page and one of its season pages. With the credentials in
fixture_spec.CREDENTIAL_VARS set (as repository secrets, for the workflow) it
also logs in and checks the catalogue, the logged-in marker the watched flags
depend on, and the account pages. Nothing is ever changed on the account.

The report carries check names, status codes and counts only -- never page
text, titles from account pages, or the account name -- because the issue it
feeds may be public.

Exit status: 0 every check passed; 1 a check failed, so the site changed in a
way this code depends on; 2 nothing failed, but something could not be
checked (site down, or this network blocked); 3 the check itself crashed.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys
import traceback
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from urllib.parse import urljoin, urlparse

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.config import SITE_URLS  # noqa: E402
from src import scraper as scraper_module  # noqa: E402
from tests import fixture_spec  # noqa: E402

PASS, FAIL, UNREACHABLE, SKIPPED = "pass", "fail", "unreachable", "skipped"
_MARK = {PASS: "✅ pass", FAIL: "❌ fail", UNREACHABLE: "⚠️ unreachable", SKIPPED: "➖ skipped"}

# Titles of the interstitial pages bot protection serves instead of the site.
# Matched on <title> only: a real login page may well mention a captcha.
_CHALLENGE_TITLE_RE = re.compile(
    r"<title>\s*(just a moment|attention required|checking your browser|ddos-guard)",
    re.IGNORECASE,
)
# How many season pages to try for one that lists episodes. A season can be
# announced in the nav before any episode is uploaded.
_SEASON_TRIES = 3
_HOME_PROBES = 3


@dataclass
class Result:
    check: str
    status: str
    detail: str


class UnreachableError(Exception):
    """The page could not be checked at all: site down, or this network blocked."""


def unreachable_reason(resp: httpx.Response) -> str | None:
    """Why a response says nothing about the site's layout, or None if it does."""
    challenged = resp.headers.get("cf-mitigated", "").lower() == "challenge" or bool(
        _CHALLENGE_TITLE_RE.search(resp.text[:20000])
    )
    if resp.status_code in (401, 403, 407, 429) or resp.status_code >= 500:
        return f"HTTP {resp.status_code}" + (" (bot check)" if challenged else "")
    if challenged:
        return f"HTTP {resp.status_code} bot-check page"
    return None


async def fetch(client: httpx.AsyncClient, url: str) -> httpx.Response:
    try:
        resp = await client.get(url, follow_redirects=True)
    except httpx.HTTPError as exc:
        raise UnreachableError(f"{type(exc).__name__} for {urlparse(url).netloc}") from exc
    reason = unreachable_reason(resp)
    if reason:
        raise UnreachableError(f"{reason} for {urlparse(url).netloc}")
    return resp


def check_login_page(html: str) -> Result:
    """The page the mirror probe accepts a host on, and the fields login posts."""
    if not scraper_module._looks_like_login_page(html):
        return Result(
            "login page",
            FAIL,
            "no password field or login form: the mirror probe would reject this host",
        )
    doc = scraper_module.make_doc(html)
    names = {str(n) for n in doc.xpath("//input/@name")} if doc is not None else set()
    missing = [f for f in fixture_spec.LOGIN_FIELDS if f not in names]
    if missing:
        return Result("login page", FAIL, f"form no longer has field(s) {', '.join(missing)}, which login posts")
    return Result("login page", PASS, f"form has {', '.join(fixture_spec.LOGIN_FIELDS)}")


def slugs_linked_from(html: str, base_url: str) -> list[str]:
    """Series slugs linked from a page, in page order, for use as probes."""
    doc = scraper_module.make_doc(html)
    if doc is None:
        return []
    slugs: list[str] = []
    for href in doc.xpath("//a/@href"):
        m = re.search(fixture_spec.SLUG_RE, urlparse(urljoin(base_url + "/", str(href))).path)
        if m and m.group(1) not in slugs:
            slugs.append(m.group(1))
    return slugs


def series_problems(parsed: dict) -> list[str]:
    problems = []
    if not parsed["title"]:
        problems.append("no title found")
    if not parsed["season_links"]:
        problems.append("no season links found")
    return problems


def episode_problems(episodes: list[dict] | None) -> list[str]:
    if episodes is None:
        return ["episode table not found, so every season would be recorded as a failed scrape"]
    if not any(ep.get("title_ger") or ep.get("title_eng") or ep.get("title") for ep in episodes):
        return [f"{len(episodes)} episodes but none has a title"]
    return []


async def find_series(client: httpx.AsyncClient, host: str) -> tuple[str, dict] | None:
    """The first probe series whose page exists, as (slug, parsed page).

    The fixed probes come first; if every one of them has gone, series linked
    from the home page are tried. Returns None when no candidate exists.
    """
    tried: list[str] = []

    async def try_slugs(slugs) -> tuple[str, dict] | None:
        for slug in slugs:
            if slug in tried:
                continue
            tried.append(slug)
            resp = await fetch(client, host + fixture_spec.SERIES_PATH.format(slug=slug))
            if resp.status_code == 404:
                continue
            parsed = fixture_spec.parse_all(resp.text, slug, host)
            if parsed["error_page"]:
                continue
            return slug, parsed
        return None

    found = await try_slugs(fixture_spec.PROBE_SLUGS)
    if found is None:
        home = await fetch(client, host + "/")
        found = await try_slugs(slugs_linked_from(home.text, host)[:_HOME_PROBES])
    return found


def pick_seasons(season_links: list) -> list[str]:
    """Season URLs to try, numbered seasons before specials and films."""
    ordered = sorted(season_links, key=lambda link: str(link[0]) in ("0", "Filme", "Specials"))
    return [url for _label, url in ordered[:_SEASON_TRIES]]


async def check_public_pages(client: httpx.AsyncClient, host: str) -> tuple[list[Result], str | None]:
    """Check a series page and one of its seasons; also return the series' slug."""
    try:
        found = await find_series(client, host)
    except UnreachableError as exc:
        return [Result("series page", UNREACHABLE, str(exc))], None
    if found is None:
        detail = "no probe series and no series linked from the home page has a page that parses"
        return [Result("series page", FAIL, detail)], None
    slug, parsed = found
    problems = series_problems(parsed)
    if problems:
        return [Result("series page", FAIL, f"{slug}: {'; '.join(problems)}")], slug
    results = [Result("series page", PASS, f"{slug}: title and {len(parsed['season_links'])} season link(s)")]

    last: list[str] = []
    for url in pick_seasons(parsed["season_links"]):
        try:
            resp = await fetch(client, url)
        except UnreachableError as exc:
            return [*results, Result("season page", UNREACHABLE, str(exc))], slug
        episodes = fixture_spec.parse_all(resp.text, slug, host)["episodes"]
        last = episode_problems(episodes)
        if episodes and not last:
            results.append(Result("season page", PASS, f"{urlparse(url).path}: {len(episodes)} episodes"))
            return results, slug
    detail = "; ".join(last) if last else "every season tried has an empty episode table"
    results.append(Result("season page", FAIL, f"{slug}: {detail}"))
    return results, slug


async def check_logged_in(host: str, slug: str | None) -> list[Result]:
    """Log in with the scraper's own code and read what only a session sees."""
    scraper = getattr(scraper_module, fixture_spec.SCRAPER_CLASS_NAME)()
    scraper.site_url = host
    try:
        client = await scraper._create_logged_in_client(verify=True)
    except httpx.HTTPError as exc:
        return [Result("login", UNREACHABLE, type(exc).__name__)]
    except RuntimeError:
        # The message can only say "check credentials"; a stale secret and a
        # changed login flow look the same from here.
        return [Result("login", FAIL, "rejected: check the credential secrets, then the login flow")]
    results = [Result("login", PASS, "logged in and verified")]
    try:
        try:
            catalogue = await scraper._get_all_series(client)
        except RuntimeError:
            results.append(Result("catalogue", FAIL, "page no longer shows the logged-in marker"))
        except httpx.HTTPError as exc:
            results.append(Result("catalogue", UNREACHABLE, type(exc).__name__))
        else:
            count = len(catalogue)
            status = PASS if count >= fixture_spec.MIN_CATALOGUE else FAIL
            results.append(
                Result("catalogue", status, f"{count} series (expected at least {fixture_spec.MIN_CATALOGUE})")
            )
        if slug:
            results.append(await _check_session_series(client, host, slug))
        results.extend(await _check_account_pages(client, host))
    finally:
        await client.aclose()
    return results


async def _check_session_series(client: httpx.AsyncClient, host: str, slug: str) -> Result:
    try:
        resp = await fetch(client, host + fixture_spec.SERIES_PATH.format(slug=slug))
    except UnreachableError as exc:
        return Result("logged-in series page", UNREACHABLE, str(exc))
    parsed = fixture_spec.parse_all(resp.text, slug, host)
    if not parsed["is_logged_in"]:
        return Result(
            "logged-in series page",
            FAIL,
            "logged-in marker not found, so every episode would be read as unwatched",
        )
    if fixture_spec.HAS_ACCOUNT_BUTTONS and (parsed["subscribed"] is None or parsed["watchlist"] is None):
        return Result("logged-in series page", FAIL, "subscribe/watchlist buttons not found")
    return Result("logged-in series page", PASS, "logged-in marker found")


async def _check_account_pages(client: httpx.AsyncClient, host: str) -> list[Result]:
    results = []
    for name in ("ACCOUNT_SUBSCRIBED_PATH", "ACCOUNT_WATCHLIST_PATH"):
        path = getattr(scraper_module, name, None)
        if not path:
            continue
        check = f"account page {path}"
        try:
            resp = await fetch(client, host + path)
        except UnreachableError as exc:
            results.append(Result(check, UNREACHABLE, str(exc)))
            continue
        doc = scraper_module.make_doc(resp.text)
        if resp.status_code == 404 or doc is None or scraper_module._check_error_page(doc):
            results.append(Result(check, FAIL, f"HTTP {resp.status_code}: page moved or gone"))
        elif not scraper_module._is_logged_in(doc):
            results.append(Result(check, FAIL, "logged-in marker not found"))
        else:
            results.append(Result(check, PASS, "reachable while logged in"))
    return results


def have_credentials() -> bool:
    return all(os.getenv(name, "").strip() for name in fixture_spec.CREDENTIAL_VARS)


async def _first_login_page(client: httpx.AsyncClient) -> tuple[str | None, str, list[str]]:
    """The first https mirror serving a login page, as (host, html, reasons others were skipped).

    Plain-http mirrors are left out: the logged-in checks would send the
    password over them.
    """
    reasons = []
    for host in (url for url in SITE_URLS if url.startswith("https://")):
        try:
            resp = await fetch(client, fixture_spec.login_url(host))
        except UnreachableError as exc:
            reasons.append(str(exc))
            continue
        if resp.status_code == 404:
            reasons.append(f"HTTP 404 for {urlparse(fixture_spec.login_url(host)).path} on {urlparse(host).netloc}")
            continue
        return host, resp.text, reasons
    return None, "", reasons


async def run_checks() -> tuple[str | None, list[Result]]:
    """Run every check against the first mirror that serves a login page."""
    ua = getattr(scraper_module, "UA", "Mozilla/5.0")
    async with httpx.AsyncClient(headers={"User-Agent": ua}, timeout=httpx.Timeout(30.0, connect=10.0)) as client:
        host, login_html, reasons = await _first_login_page(client)
        if host is None:
            # Every mirror answering 404 means the login page moved; anything
            # else in the mix means at least one mirror could not be asked.
            status = FAIL if reasons and all("HTTP 404" in r for r in reasons) else UNREACHABLE
            return None, [Result("login page", status, "; ".join(reasons) or "no https mirror configured")]
        results = [check_login_page(login_html)]
        public, slug = await check_public_pages(client, host)
        results.extend(public)

    if have_credentials():
        results.extend(await check_logged_in(host, slug))
    else:
        names = " and ".join(fixture_spec.CREDENTIAL_VARS)
        results.append(Result("logged-in checks", SKIPPED, f"set {names} to log in and check the catalogue"))
    return host, results


def exit_code(results: list[Result]) -> int:
    statuses = {r.status for r in results}
    if FAIL in statuses:
        return 1
    if UNREACHABLE in statuses:
        return 2
    return 0


def render(host: str | None, results: list[Result], today: date) -> str:
    where = urlparse(host).netloc if host else "no reachable mirror"
    lines = [
        f"### Site check: {where}, {today.isoformat()}",
        "",
        "| Check | Result | Detail |",
        "| --- | --- | --- |",
    ]
    for r in results:
        lines.append(f"| {r.check} | {_MARK[r.status]} | {r.detail.replace('|', '/')} |")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Check the live site for changes that would break this scraper.")
    ap.add_argument("--report", help="also write the markdown report to this file")
    args = ap.parse_args(argv)
    try:
        host, results = asyncio.run(run_checks())
        report, code = render(host, results, date.today()), exit_code(results)
    except Exception:  # noqa: BLE001 -- reported as a crash, not mistaken for a failed check
        traceback.print_exc()
        report, code = "### Site check crashed\n\nSee the workflow log for the traceback.\n", 3
    print(report)
    if args.report:
        Path(args.report).write_text(report, encoding="utf-8")
    return code


if __name__ == "__main__":
    sys.exit(main())
