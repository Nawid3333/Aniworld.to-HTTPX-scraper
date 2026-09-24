"""Which parser outputs the golden fixtures pin, for this site.

Each project defines the same names so `capture_fixtures.py`,
`test_golden_parse.py` and `site_check.py` stay identical across the three
scrapers; only this adapter differs.
"""

from src.scraper import (  # noqa: E402
    _check_error_page,
    _detect_subscription_status,
    _extract_alt_titles,
    _extract_season_links,
    _extract_title,
    _is_logged_in,
    _login_url,
    _parse_episodes,
    make_doc,
)

SCRAPER_CLASS_NAME = "AniWorldScraper"
SLUG_RE = r"/anime/stream/([^/?#]+)"
SERIES_PATH = "/anime/stream/{slug}"
CATALOGUE_PATH = "/animes"

# ── Live site check (tests/site_check.py) ──────────────────────────────────
# The login form fields _login_client posts (it also sends autoLogin=on,
# which the site accepts without a matching field).
LOGIN_FIELDS = ("email", "password")
# Read by config.config; set as repository secrets for the monthly workflow.
CREDENTIAL_VARS = ("ANIWORLD_EMAIL", "ANIWORLD_PASSWORD")
# Long-running series that should outlive any one check. A slug that has
# gone is skipped, and series linked from the home page are tried after these.
PROBE_SLUGS = ("one-piece", "naruto", "jujutsu-kaisen")
# The index held 2,490 series in September 2026. Far below that, the
# catalogue parse is losing series rather than the site shrinking.
MIN_CATALOGUE = 1500
# Series pages carry subscribe and watchlist controls (_detect_subscription_status).
HAS_ACCOUNT_BUTTONS = True


def login_url(site_url: str) -> str:
    return _login_url(site_url)


def parse_all(html: str, slug: str, base_url: str) -> dict:
    """Run every parser this scraper applies to a page, as a plain dict.

    Plain data only -- the golden file has to survive a parser swap, so it
    records what the scrapers actually store, not parse-tree objects. That is
    exactly what it was for: the recorded file predates the move off
    BeautifulSoup and was left untouched across it, so these tests re-parse
    every captured page with the lxml helpers and compare against what the
    soup ones produced.
    """
    doc = make_doc(html)
    subscribed, watchlist = _detect_subscription_status(doc)
    return {
        "is_logged_in": _is_logged_in(doc),
        "error_page": _check_error_page(doc),
        "title": _extract_title(doc),
        "alt_titles": _extract_alt_titles(doc),
        "subscribed": subscribed,
        "watchlist": watchlist,
        "season_links": [list(x) for x in _extract_season_links(doc, slug, base_url)],
        "episodes": _parse_episodes(html),
    }
