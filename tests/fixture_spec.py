"""Which parser outputs the golden fixtures pin, for this site.

Each project defines the same two names so `capture_fixtures.py` and
`test_golden_parse.py` stay identical across the three scrapers; only this
adapter differs.
"""

from src.scraper import (  # noqa: E402
    _check_error_page,
    _detect_subscription_status,
    _extract_alt_titles,
    _extract_season_links,
    _extract_title,
    _is_logged_in,
    _parse_episodes,
    make_soup,
)

SCRAPER_CLASS_NAME = "AniWorldScraper"
SLUG_RE = r"/anime/stream/([^/?#]+)"
SERIES_PATH = "/anime/stream/{slug}"
CATALOGUE_PATH = "/animes"


def parse_all(html: str, slug: str, base_url: str) -> dict:
    """Run every parser this scraper applies to a page, as a plain dict.

    Plain data only -- the golden file has to survive a parser swap, so it
    records what the scrapers actually store, not BeautifulSoup objects.
    """
    soup = make_soup(html)
    subscribed, watchlist = _detect_subscription_status(soup)
    return {
        "is_logged_in": _is_logged_in(soup),
        "error_page": _check_error_page(soup),
        "title": _extract_title(soup),
        "alt_titles": _extract_alt_titles(soup),
        "subscribed": subscribed,
        "watchlist": watchlist,
        "season_links": [list(x) for x in _extract_season_links(soup, slug, base_url)],
        "episodes": _parse_episodes(soup),
    }
