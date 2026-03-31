"""
AniWorld.to Anime Scraper — powered by httpx (no browser needed).
"""

import asyncio
import json
import logging
import os
import re
import threading
import time
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup

from config.config import (
    EMAIL, PASSWORD, DATA_DIR, SERIES_INDEX_FILE, NUM_WORKERS,
    HTTP_REQUEST_TIMEOUT,
)

logger = logging.getLogger(__name__)

# ── Constants ───────────────────────────────────────────────────────────────
SITE_URL = "https://aniworld.to"
LOGIN_URL = f"{SITE_URL}/login"
SERIES_LIST_URL = f"{SITE_URL}/animes"
ACCOUNT_SUBSCRIBED_URL = f"{SITE_URL}/account/subscribed"
ACCOUNT_WATCHLIST_URL = f"{SITE_URL}/account/watchlist"
CHECKPOINT_EVERY = 10
REQUEST_TIMEOUT = HTTP_REQUEST_TIMEOUT
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0"

_ANIME_PATH_RE = re.compile(r'(/anime/stream/[^/]+)')
_ANIME_SLUG_RE = re.compile(r'^/anime/stream/([^/?#]+)/?$')
_STAFFEL_RE = re.compile(r'/staffel-(\d+)')
_EPISODE_LABEL_RE = re.compile(r'\s*\[Episode\s+\d+\]\s*$', re.IGNORECASE)

# Error page detection — matches titles like "404", "Error 404", "502 Bad Gateway"
_ERROR_TITLE_RE = re.compile(
    r'^(?:Error\s+)?(?P<code>\d{3})\b|\b(?:Error|Fehler)\s+(?P<code2>\d{3})\b',
    re.IGNORECASE,
)
_SERVER_ERROR_CODES = {'429', '500', '502', '503', '504'}

# Language flag filename → language code
_LANGUAGE_MAP = {
    "german.svg": "german_dub",
    "japanese-german.svg": "german_sub",
    "japanese-english.svg": "english_sub",
}


def _is_logged_in(html: str) -> bool:
    """Check if the page indicates a logged-in session."""
    soup = BeautifulSoup(html, "html.parser")
    return soup.select_one("div.avatar a[href*='/user/profil/']") is not None


def _check_error_page(html: str) -> str | None:
    """Detect HTTP error pages (404, 502, etc.) returned as HTML.

    Returns an error string like '404' if an error page is detected, None otherwise.
    """
    soup = BeautifulSoup(html, "html.parser")
    # If the page has series content (season nav), it's a real series page
    if soup.select_one("#stream ul li a[href*='/staffel-']") or soup.select_one("#stream ul li a[href*='/filme']"):
        return None
    title_tag = soup.find("title")
    if title_tag:
        title_text = title_tag.get_text(strip=True)
        m = _ERROR_TITLE_RE.search(title_text)
        if m:
            return m.group("code") or m.group("code2")
    h2_tag = soup.find("h2")
    if h2_tag and h2_tag.get_text(strip=True).isdigit():
        code = h2_tag.get_text(strip=True)
        if len(code) == 3:
            return code
    return None


# ── HTML helpers ────────────────────────────────────────────────────────────

def _parse_episodes(html: str) -> list[dict] | None:
    """Parse episode rows from a season page.

    Uses aniworld.to-specific selectors:
      - Table: table.seasonEpisodesList tbody tr[data-episode-id]
      - Number: meta[itemprop='episodeNumber'] content attr
      - Title (DE): td.seasonEpisodeTitle a strong
      - Title (EN): td.seasonEpisodeTitle a span (strip [Episode N] suffix)
      - Watched: 'seen' class on the row
      - Languages: td.editFunctions img.flag
    Falls back to generic selectors if primary ones fail.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Primary: aniworld.to episode table
    rows = soup.select("table.seasonEpisodesList tbody tr[data-episode-id]")
    if not rows:
        rows = soup.select("table.seasonEpisodesList tr[data-episode-id]")
    if not rows:
        rows = soup.select("tbody tr[data-episode-id]")
    if not rows:
        return []

    episodes = []
    for idx, row in enumerate(rows, start=1):
        # Episode number from meta tag
        ep_num_el = row.select_one("meta[itemprop='episodeNumber']")
        if ep_num_el:
            ep_num = ep_num_el.get("content", "")
        else:
            ep_num = ""
        if not ep_num:
            # Filme/movie pages lack meta episodeNumber — fall back to
            # data-episode-season-id
            ep_num = row.get("data-episode-season-id", "")
        if not ep_num:
            logger.warning("Could not determine episode number for row %d", idx)
            return None

        try:
            ep_num_int = int(ep_num)
        except ValueError:
            logger.warning("Non-numeric episode number '%s' in row %d", ep_num, idx)
            return None

        # German title
        ger_el = row.select_one("td.seasonEpisodeTitle a strong")
        title_ger = ger_el.get_text(strip=True) if ger_el else ""

        # English title (strip [Episode NNN] suffix)
        eng_el = row.select_one("td.seasonEpisodeTitle a span")
        title_eng = eng_el.get_text(strip=True) if eng_el else ""
        title_eng = _EPISODE_LABEL_RE.sub("", title_eng).strip()

        # Watched status from row class
        row_classes = row.get("class") or []
        watched = "seen" in row_classes

        # Language flags
        flag_imgs = row.select("td.editFunctions img.flag")
        languages = []
        for img in flag_imgs:
            src = img.get("src", "")
            title_attr = img.get("title", "")
            flag_file = src.rsplit("/", 1)[-1] if "/" in src else src
            lang = _LANGUAGE_MAP.get(flag_file, title_attr or flag_file)
            if lang:
                languages.append(lang)

        ep = {"number": ep_num_int, "watched": watched}
        if title_ger:
            ep["title_ger"] = title_ger
        if title_eng:
            ep["title_eng"] = title_eng
        if languages:
            ep["languages"] = languages
        episodes.append(ep)
    return episodes


def _extract_season_links(html: str, series_slug: str) -> list[tuple[str, str]]:
    """Extract season numbers and URLs from the #stream season navigation.

    Handles staffel-N seasons and Filme (movies/specials).
    """
    soup = BeautifulSoup(html, "html.parser")
    seasons = []
    seen = set()

    # Primary: href pattern /anime/stream/{slug}/staffel-{num}
    for a_tag in soup.select("#stream ul:first-of-type li a[href*='/staffel-']"):
        href = a_tag.get("href", "")
        m = _STAFFEL_RE.search(href)
        if m and m.group(1) not in seen:
            season_num = m.group(1)
            seen.add(season_num)
            url = f"{SITE_URL}/anime/stream/{series_slug}/staffel-{season_num}"
            seasons.append((season_num, url))

    # Check for Filme (movies/OVAs/specials)
    for a_tag in soup.select("#stream ul:first-of-type li a[href*='/filme']"):
        if "Filme" not in seen:
            seen.add("Filme")
            url = f"{SITE_URL}/anime/stream/{series_slug}/filme"
            seasons.append(("Filme", url))

    if seasons:
        return seasons

    # Fallback: any staffel links on the page
    staffel_pattern = re.compile(
        rf'/anime/stream/{re.escape(series_slug)}/staffel-(\d+)', re.IGNORECASE
    )
    for a_tag in soup.find_all("a", href=True):
        m = staffel_pattern.search(a_tag["href"])
        if m and m.group(1) not in seen:
            seen.add(m.group(1))
            url = f"{SITE_URL}/anime/stream/{series_slug}/staffel-{m.group(1)}"
            seasons.append((m.group(1), url))

    return seasons


def _extract_title(html: str) -> str | None:
    """Extract series title from the page.

    Tries h1[itemprop='name'] > span first (aniworld.to), then h1.fw-bold, then h2.
    """
    soup = BeautifulSoup(html, "html.parser")
    # aniworld.to: h1[itemprop='name'] > span
    h1_span = soup.select_one("h1[itemprop='name'] > span")
    if h1_span:
        text = h1_span.get_text(strip=True)
        if text:
            return text
    # Fallback: h1.fw-bold
    h1 = soup.select_one("h1.fw-bold")
    if h1:
        text = h1.get_text(strip=True)
        if text:
            return text
    # Final fallback: h2
    h2 = soup.find("h2")
    if h2:
        text = h2.get_text(strip=True)
        text = re.sub(r'\s*Staffel\s*\d+.*$', '', text)
        return text or None
    return None


def _extract_alt_titles(html: str) -> list[str]:
    """Extract alternative titles from the series detail page."""
    soup = BeautifulSoup(html, "html.parser")
    alt_el = soup.select_one("h1[itemprop='name']")
    if alt_el:
        alt_raw = alt_el.get("data-alternativetitles", "")
        if alt_raw:
            return [t.strip() for t in alt_raw.split(",") if t.strip()]
    return []


def _extract_description(html: str) -> str:
    """Extract series description from the detail page."""
    soup = BeautifulSoup(html, "html.parser")
    desc_el = soup.select_one("p.seri_des")
    if desc_el:
        return desc_el.get("data-full-description", "") or desc_el.get_text(strip=True)
    return ""


def _detect_subscription_status(html: str) -> tuple[bool | None, bool | None]:
    """Detect subscription and watchlist status from a series page.

    Uses aniworld.to's div.add-series container with data-series-favourite
    and data-series-watchlist attributes (value "1" = active).
    Cross-validates with CSS classes li.setFavourite.true / li.setWatchlist.true.

    Returns (subscribed, watchlist) — None if container not found or not logged in.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Verify logged-in state (profile avatar)
    if not soup.select_one("div.avatar a[href^='/user/profil/']"):
        return (None, None)

    # Find subscription container
    container = soup.select_one("div.add-series")
    if not container:
        return (None, None)

    subscribed = None
    watchlist = None

    # Primary: data attributes (most reliable)
    fav_val = container.get("data-series-favourite")
    wl_val = container.get("data-series-watchlist")
    if fav_val is not None:
        subscribed = fav_val == "1"
    if wl_val is not None:
        watchlist = wl_val == "1"

    # Cross-validate with CSS classes
    css_subscribed = soup.select_one("li.setFavourite.true") is not None
    css_watchlist = soup.select_one("li.setWatchlist.true") is not None

    if subscribed is not None and css_subscribed != subscribed:
        logger.warning("Subscribe mismatch: data-attr=%s, CSS=%s — trusting data attribute", subscribed, css_subscribed)
    if watchlist is not None and css_watchlist != watchlist:
        logger.warning("Watchlist mismatch: data-attr=%s, CSS=%s — trusting data attribute", watchlist, css_watchlist)

    # Fallback to CSS if data attributes missing
    if subscribed is None:
        subscribed = css_subscribed
    if watchlist is None:
        watchlist = css_watchlist

    return (subscribed, watchlist)


# ── Exception ───────────────────────────────────────────────────────────────

class ScrapingPaused(Exception):
    pass


# ── AniWorldScraper (httpx) ────────────────────────────────────────────────

class AniWorldScraper:
    """AniWorld.to anime scraper powered by httpx (no browser needed)."""

    def __init__(self):
        self.series_data: list[dict] = []
        self.all_discovered_series: list[dict] | None = None
        self.completed_links: set[str] = set()
        self.failed_links: list[dict] = []

        self.checkpoint_file = os.path.join(DATA_DIR, '.scrape_checkpoint.json')
        self.failed_file = os.path.join(DATA_DIR, '.failed_series.json')
        self.ignore_file = os.path.join(DATA_DIR, '.ignored_series.json')
        self.ignored_seasons_file = os.path.join(DATA_DIR, '.ignored_seasons.json')
        self.pause_file = os.path.join(DATA_DIR, '.pause_scraping')

        self._checkpoint_mode: str | None = None
        self._use_parallel: bool = True
        self._lock = threading.Lock()
        self._last_pause_check = 0.0
        self._pause_cached = False
        self.paused = False
        self._ignored_seasons_cache: set[tuple[str, str]] | None = None
        self._stale_ignored_warnings: list[dict] = []

    # ── Static / class methods ──────────────────────────────────────────────

    @staticmethod
    def get_checkpoint_mode(data_dir):
        cp_file = os.path.join(data_dir, '.scrape_checkpoint.json')
        try:
            if os.path.exists(cp_file):
                with open(cp_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    return data.get('mode')
        except Exception:
            pass
        return None

    # ── Checkpoint management ───────────────────────────────────────────────

    def save_checkpoint(self, include_data=False):
        with self._lock:
            payload = {
                'completed_links': list(self.completed_links),
                'mode': self._checkpoint_mode,
                'timestamp': time.time(),
            }
            if include_data:
                payload['series_data'] = self.series_data
            tmp = self.checkpoint_file + '.tmp'
            try:
                with open(tmp, 'w', encoding='utf-8') as f:
                    json.dump(payload, f, ensure_ascii=False)
                os.replace(tmp, self.checkpoint_file)
            except Exception as e:
                logger.error("Failed to save checkpoint: %s", e)

    def load_checkpoint(self) -> bool:
        with self._lock:
            try:
                if not os.path.exists(self.checkpoint_file):
                    return False
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    self.completed_links = set(data.get('completed_links', []))
                    self._checkpoint_mode = data.get('mode')
                    saved_data = data.get('series_data')
                    if saved_data:
                        self.series_data = saved_data
                elif isinstance(data, list):
                    self.completed_links = set(data)
                return bool(self.completed_links)
            except Exception as e:
                logger.error("Failed to load checkpoint: %s", e)
                return False

    def clear_checkpoint(self):
        with self._lock:
            try:
                if os.path.exists(self.checkpoint_file):
                    os.remove(self.checkpoint_file)
            except OSError:
                pass

    # ── Failed series management ────────────────────────────────────────────

    def save_failed_series(self):
        with self._lock:
            existing = []
            try:
                if os.path.exists(self.failed_file):
                    with open(self.failed_file, 'r', encoding='utf-8') as f:
                        existing = json.load(f)
            except Exception:
                pass
            seen = {e.get('url') for e in existing if isinstance(e, dict)}
            for item in self.failed_links:
                if isinstance(item, dict) and item.get('url') not in seen:
                    existing.append(item)
                    seen.add(item.get('url'))
            tmp = self.failed_file + '.tmp'
            try:
                with open(tmp, 'w', encoding='utf-8') as f_out:
                    json.dump(existing, f_out, indent=2, ensure_ascii=False)
                os.replace(tmp, self.failed_file)
            except Exception as e:
                logger.error("Failed to save failed series: %s", e)

    def load_failed_series(self) -> list:
        with self._lock:
            try:
                if os.path.exists(self.failed_file):
                    with open(self.failed_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    if isinstance(data, list):
                        return data
            except Exception:
                pass
            return []

    def clear_failed_series(self):
        with self._lock:
            try:
                if os.path.exists(self.failed_file):
                    os.remove(self.failed_file)
            except OSError:
                pass

    # ── Ignore list management ──────────────────────────────────────────────

    def load_ignored_series(self) -> list[dict]:
        try:
            if os.path.exists(self.ignore_file):
                with open(self.ignore_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if isinstance(data, list):
                    return data
        except Exception:
            pass
        return []

    def save_ignored_series(self, ignored: list[dict]):
        tmp = self.ignore_file + '.tmp'
        try:
            with open(tmp, 'w', encoding='utf-8') as f:
                json.dump(ignored, f, indent=2, ensure_ascii=False)
            os.replace(tmp, self.ignore_file)
        except Exception as e:
            logger.error("Failed to save ignored series: %s", e)

    def get_ignored_slugs(self) -> set[str]:
        return {self.get_series_slug_from_url(s.get('url', '')) for s in self.load_ignored_series()} - {'unknown'}

    # ── Ignored seasons management ──────────────────────────────────────────

    def load_ignored_seasons(self) -> list[dict]:
        try:
            if os.path.exists(self.ignored_seasons_file):
                with open(self.ignored_seasons_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if isinstance(data, list):
                    return data
        except Exception:
            pass
        return []

    def save_ignored_seasons(self, ignored: list[dict]):
        tmp = self.ignored_seasons_file + '.tmp'
        try:
            with open(tmp, 'w', encoding='utf-8') as f:
                json.dump(ignored, f, indent=2, ensure_ascii=False)
            os.replace(tmp, self.ignored_seasons_file)
        except Exception as e:
            logger.error("Failed to save ignored seasons: %s", e)

    def get_ignored_seasons_set(self) -> set[tuple[str, str]]:
        """Return set of (slug, season) tuples that have episode 0 ignored."""
        return {(e.get('slug', ''), str(e.get('season', ''))) for e in self.load_ignored_seasons()} - {('', '')}

    def _get_ignored_seasons(self) -> set[tuple[str, str]]:
        """Cached version — loads from file once per run."""
        if self._ignored_seasons_cache is None:
            self._ignored_seasons_cache = self.get_ignored_seasons_set()
        return self._ignored_seasons_cache

    # ── URL helpers ─────────────────────────────────────────────────────────

    def get_series_slug_from_url(self, url):
        try:
            path = urlparse(url).path if url.startswith('http') else url
            parts = path.split('/')
            # /anime/stream/{slug}
            if 'stream' in parts:
                idx = parts.index('stream')
                if idx + 1 < len(parts) and parts[idx + 1]:
                    return parts[idx + 1]
            return 'unknown'
        except Exception:
            return 'unknown'

    def normalize_to_series_url(self, url):
        if not url:
            return url
        url = url.split('?')[0].split('#')[0]
        m = _ANIME_PATH_RE.search(url)
        if m:
            return f"{SITE_URL}{m.group(1)}"
        # Bare slug
        slug = url.strip().strip('/')
        if slug and not slug.startswith('http'):
            return f"{SITE_URL}/anime/stream/{slug}"
        return url

    # ── Pause detection ─────────────────────────────────────────────────────

    def _check_pause(self):
        now = time.time()
        if now - self._last_pause_check < 5:
            return self._pause_cached
        self._last_pause_check = now
        self._pause_cached = os.path.exists(self.pause_file)
        return self._pause_cached

    def _clear_pause_file(self):
        try:
            if os.path.exists(self.pause_file):
                os.remove(self.pause_file)
        except OSError:
            pass

    # ── Index helpers (for new_only mode) ───────────────────────────────────

    def load_existing_slugs(self) -> set[str]:
        existing = set()
        try:
            if os.path.exists(SERIES_INDEX_FILE):
                with open(SERIES_INDEX_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                items = data if isinstance(data, list) else list(data.values())
                for item in items:
                    url = item.get('url', '') or item.get('link', '')
                    if url:
                        existing.add(self.get_series_slug_from_url(url))
        except Exception:
            pass
        existing.discard('unknown')
        return existing

    # ── Async internals ─────────────────────────────────────────────────────

    async def _create_logged_in_client(self) -> httpx.AsyncClient:
        client = httpx.AsyncClient(
            headers={
                "User-Agent": UA,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "de,en-US;q=0.7,en;q=0.3",
                "Accept-Encoding": "gzip, deflate, br",
                "Upgrade-Insecure-Requests": "1",
            },
            timeout=httpx.Timeout(REQUEST_TIMEOUT, connect=10.0),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=2, max_keepalive_connections=1),
        )
        # GET the login page first to establish session cookies
        await client.get(LOGIN_URL)

        # aniworld.to login: email + password + autoLogin (no CSRF token)
        login_data = {
            "email": EMAIL,
            "password": PASSWORD,
            "autoLogin": "on",
        }

        await client.post(
            LOGIN_URL,
            data=login_data,
            headers={
                "Origin": SITE_URL,
                "Referer": LOGIN_URL,
                "Content-Type": "application/x-www-form-urlencoded",
            },
            follow_redirects=True,
        )
        # POST returns empty body — session is verified on first real request

        return client

    async def _get_all_series(self, client: httpx.AsyncClient) -> list[dict]:
        """Fetch the full anime catalogue from aniworld.to/animes."""
        resp = await client.get(SERIES_LIST_URL)
        if not _is_logged_in(resp.text):
            raise RuntimeError("Not logged in — cannot fetch anime catalogue")
        soup = BeautifulSoup(resp.text, "html.parser")
        series, seen_slugs = [], set()

        # Primary: #seriesContainer ul li a
        for a in soup.select("#seriesContainer ul li a"):
            href = a.get("href", "")
            m = _ANIME_SLUG_RE.match(href)
            if not m:
                continue
            slug = m.group(1)
            if slug in seen_slugs:
                continue
            seen_slugs.add(slug)
            title = a.get_text(strip=True)
            if not title:
                continue

            # Extract alternative titles from data attribute
            alt_titles_raw = a.get("data-alternative-title", "")
            alt_titles = [t.strip() for t in alt_titles_raw.split(",") if t.strip()] if alt_titles_raw else []

            series.append({
                "title": title,
                "link": f"/anime/stream/{slug}",
                "url": f"{SITE_URL}/anime/stream/{slug}",
                "alt_titles": alt_titles,
            })

        # Fallback: scan all links if container selector failed
        if not series:
            for a in soup.find_all("a", href=True):
                href = a["href"]
                m = _ANIME_SLUG_RE.match(href)
                if not m:
                    continue
                slug = m.group(1)
                if slug in seen_slugs:
                    continue
                seen_slugs.add(slug)
                title = a.get_text(strip=True)
                if not title:
                    continue
                series.append({
                    "title": title,
                    "link": f"/anime/stream/{slug}",
                    "url": f"{SITE_URL}/anime/stream/{slug}",
                })

        return series

    async def _get_account_series(self, client: httpx.AsyncClient,
                                  source: str = 'both') -> list[dict]:
        """Fetch subscribed/watchlist anime from account pages.

        Args:
            source: 'subscribed', 'watchlist', or 'both'

        Returns list of series dicts with title, link, url keys.
        Note: aniworld.to account pages have no pagination.
        """
        pages = []
        if source in ('subscribed', 'both'):
            pages.append((ACCOUNT_SUBSCRIBED_URL, 'Subscriptions'))
        if source in ('watchlist', 'both'):
            pages.append((ACCOUNT_WATCHLIST_URL, 'Watchlist'))

        seen_slugs = set()
        series_list = []

        for base_url, label in pages:
            count_before = len(series_list)
            try:
                resp = await client.get(base_url, follow_redirects=True)
            except httpx.HTTPError as e:
                logger.warning("Could not fetch %s: %s", base_url, e)
                continue

            if not _is_logged_in(resp.text):
                raise RuntimeError(f"Not logged in — cannot fetch {label} page")

            soup = BeautifulSoup(resp.text, "html.parser")

            # aniworld.to: div.seriesListContainer > div a with h3 for title
            for entry in soup.select("div.seriesListContainer > div a"):
                href = entry.get("href", "")
                m = _ANIME_SLUG_RE.match(href)
                if not m:
                    continue
                slug = m.group(1)
                if slug in seen_slugs:
                    continue
                seen_slugs.add(slug)

                title_el = entry.select_one("h3")
                title = title_el.get_text(strip=True) if title_el else slug

                series_list.append({
                    "title": title,
                    "link": f"/anime/stream/{slug}",
                    "url": f"{SITE_URL}/anime/stream/{slug}",
                })

            # Fallback: generic link scan
            if len(series_list) == count_before:
                for link in soup.find_all("a", href=True):
                    href = link.get("href", "")
                    m = _ANIME_SLUG_RE.match(href)
                    if not m:
                        continue
                    slug = m.group(1)
                    if slug in seen_slugs:
                        continue
                    seen_slugs.add(slug)
                    title = link.get_text(strip=True) or slug
                    series_list.append({
                        "title": title,
                        "link": f"/anime/stream/{slug}",
                        "url": f"{SITE_URL}/anime/stream/{slug}",
                    })

            count_after = len(series_list)
            print(f"  ✓ {label}: {count_after - count_before} anime found")

        return series_list

    async def _scrape_one_series(self, client: httpx.AsyncClient, info: dict) -> dict:
        """Scrape a single anime: all seasons, episodes, subscription status."""
        url = info["url"]
        slug = self.get_series_slug_from_url(url)

        try:
            resp = await client.get(url, follow_redirects=True)
        except httpx.HTTPError as e:
            return self._error_result(info, str(e))

        html = resp.text

        # Detect error pages (404, 502, etc.) before parsing content
        error_code = _check_error_page(html)
        if error_code:
            reason = f"{error_code} server error" if error_code in _SERVER_ERROR_CODES else f"{error_code} error page"
            logger.warning("Error page detected for %s: %s", url, error_code)
            return self._error_result(info, reason)

        # Verify still logged in
        if not _is_logged_in(html):
            logger.error("Session expired while scraping %s", url)
            return self._error_result(info, "session expired — not logged in")

        title = _extract_title(html) or info.get("title", slug)

        # Detect subscription/watchlist status from the main series page
        subscribed, watchlist = _detect_subscription_status(html)

        # Extract alt titles and description
        alt_titles_from_page = _extract_alt_titles(html)
        # Merge with alt_titles from index page if present
        alt_titles_from_info = info.get("alt_titles", [])
        alt_titles = list(dict.fromkeys(alt_titles_from_info + alt_titles_from_page))

        description = _extract_description(html)

        season_links = _extract_season_links(html, slug)
        if not season_links:
            return self._error_result(info, "no seasons found")

        seasons_data = []
        total_watched, total_eps = 0, 0
        ignored_seasons = self._get_ignored_seasons()
        has_episode_zero = False
        stale_ignored = []

        for label, season_url in season_links:
            try:
                sr = await client.get(season_url, follow_redirects=True)
            except httpx.HTTPError as e:
                return self._error_result(info, f"season {label} fetch failed: {e}")
            episodes = _parse_episodes(sr.text)
            if episodes is None:
                return self._error_result(info, f"season {label} episode parse failed")

            ep0_exists = any(ep["number"] == 0 for ep in episodes)
            is_ignored = (slug, label) in ignored_seasons

            if ep0_exists and is_ignored:
                # Already in ignored list — silently filter out episode 0
                episodes = [ep for ep in episodes if ep["number"] != 0]
            elif ep0_exists and not is_ignored:
                # New episode 0 — flag for warning + failed_links
                has_episode_zero = True
            elif not ep0_exists and is_ignored:
                # Stale: episode 0 no longer exists but still in ignored list
                stale_ignored.append({"slug": slug, "season": label})

            watched_count = sum(1 for ep in episodes if ep["watched"])
            total_count = len(episodes)
            season_entry = {
                "season": label,
                "url": season_url,
                "episodes": episodes,
                "watched_episodes": watched_count,
                "total_episodes": total_count,
            }
            if ep0_exists and is_ignored:
                season_entry["ignored_episode_0"] = True
            seasons_data.append(season_entry)
            total_watched += watched_count
            total_eps += total_count

        result = {
            "title": title,
            "link": info["link"],
            "url": info["url"],
            "total_seasons": len(seasons_data),
            "total_episodes": total_eps,
            "watched_episodes": total_watched,
            "unwatched_episodes": max(0, total_eps - total_watched),
            "subscribed": subscribed,
            "watchlist": watchlist,
            "seasons": seasons_data,
        }
        if alt_titles:
            result["alt_titles"] = alt_titles
        if description:
            result["description"] = description
        if has_episode_zero:
            result["_has_episode_zero"] = True
        if stale_ignored:
            result["_stale_ignored_seasons"] = stale_ignored
        return result

    @staticmethod
    def _error_result(info: dict, reason: str) -> dict:
        return {
            "title": f"[ERROR: {reason}]",
            "link": info.get("link", ""),
            "url": info.get("url", ""),
            "total_seasons": 0,
            "total_episodes": 0,
            "watched_episodes": 0,
            "unwatched_episodes": 0,
            "subscribed": None,
            "watchlist": None,
            "seasons": [],
        }

    # ── Worker ──────────────────────────────────────────────────────────────

    async def _worker(self, worker_id: int, queue: asyncio.Queue,
                      results: list, progress: dict, total: int):
        try:
            client = await self._create_logged_in_client()
        except RuntimeError:
            logger.warning("Worker %d login failed, retrying...", worker_id)
            await asyncio.sleep(1)
            try:
                client = await self._create_logged_in_client()
            except RuntimeError:
                logger.error("Worker %d login failed permanently", worker_id)
                return

        try:
            while True:
                if self._check_pause():
                    raise ScrapingPaused("Pause file detected")

                try:
                    info = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                result = await self._scrape_one_series(client, info)

                if result["title"].startswith("[ERROR"):
                    self.failed_links.append({
                        "url": info["url"],
                        "title": info.get("title", ""),
                        "link": info.get("link", ""),
                        "reason": "scrape_error",
                    })
                elif result.get("total_episodes", 0) == 0:
                    results.append(result)
                    self.failed_links.append({
                        "url": info["url"],
                        "title": result.get("title", info.get("title", "")),
                        "link": info.get("link", ""),
                        "reason": "zero_episodes",
                    })
                else:
                    results.append(result)
                    if result.get("_has_episode_zero"):
                        self.failed_links.append({
                            "url": info["url"],
                            "title": result.get("title", info.get("title", "")),
                            "link": info.get("link", ""),
                            "reason": "episode_0_placeholder",
                        })
                    if result.get("_stale_ignored_seasons"):
                        with self._lock:
                            for entry in result["_stale_ignored_seasons"]:
                                self._stale_ignored_warnings.append({
                                    "title": result.get("title", ""),
                                    "slug": entry["slug"],
                                    "season": entry["season"],
                                })

                link = info.get("link", "")
                if link:
                    self.completed_links.add(link)

                progress["done"] += 1
                done = progress["done"]

                # Progress bar + ETA
                elapsed = time.perf_counter() - progress["start"]
                rate = done / elapsed if elapsed > 0 else 0
                eta = (total - done) / rate if rate > 0 else 0
                eta_mins = f"{eta / 60:.1f}"
                pct = int((done / total) * 100)
                bar_len = 30
                filled = int(bar_len * done / total)
                bar = '█' * filled + '░' * (bar_len - filled)

                season_labels = [s.get('season', '?') for s in result.get('seasons', [])]
                season_info = f" [{','.join(season_labels)}]" if season_labels else ""

                # Subscription status indicators
                sub_parts = []
                if result.get("subscribed") is not None:
                    sub_parts.append(f"Sub:{'✓' if result['subscribed'] else '✗'}")
                if result.get("watchlist") is not None:
                    sub_parts.append(f"WL:{'✓' if result['watchlist'] else '✗'}")
                sub_info = f" ({' '.join(sub_parts)})" if sub_parts else ""

                ep0_warn = " ⚠ episode 0 detected" if result.get("_has_episode_zero") else ""

                if result["title"].startswith("[ERROR"):
                    print(
                        f"[{done}/{total}] [{bar}] {pct}% | ETA: {eta_mins}m"
                        f" | ⚠ {info.get('title', '?')}: Failed"
                    )
                elif result["total_episodes"] == 0:
                    print(
                        f"[{done}/{total}] [{bar}] {pct}% | ETA: {eta_mins}m"
                        f" | ⚠ {result['title']}{season_info}: No episodes{sub_info}"
                    )
                else:
                    print(
                        f"[{done}/{total}] [{bar}] {pct}% | ETA: {eta_mins}m"
                        f" | ✓ {result['title']}{season_info}:"
                        f" {result['watched_episodes']}/{result['total_episodes']}"
                        f" watched{sub_info}{ep0_warn}"
                    )

                if done % CHECKPOINT_EVERY == 0:
                    self.series_data = list(results)
                    self.save_checkpoint(include_data=True)
        finally:
            await client.aclose()

    # ── Async scrape orchestrators ──────────────────────────────────────────

    def _filter_completed(self, series_list: list[dict]) -> list[dict] | None:
        if not self.completed_links:
            return series_list
        before = len(series_list)
        filtered = [s for s in series_list if s.get('link') not in self.completed_links]
        if before != len(filtered):
            print(f"  Skipping {before - len(filtered)} already-completed anime")
        if not filtered:
            print("✓ All anime already scraped (from checkpoint)")
            return None
        return filtered

    async def _scrape_list(self, series_list: list[dict], num_workers: int | None = None):
        """Scrape a list of anime using multi-session workers."""
        filtered = self._filter_completed(series_list)
        if filtered is None:
            return

        queue: asyncio.Queue = asyncio.Queue()
        for s in filtered:
            queue.put_nowait(s)

        results: list[dict] = list(self.series_data)  # keep checkpoint data
        n = min(num_workers or NUM_WORKERS, len(filtered))
        progress = {"done": 0, "start": time.perf_counter()}

        print(f"→ Scraping {len(filtered)} anime with {n} session(s)...")

        tasks = [
            self._worker(i, queue, results, progress, len(filtered))
            for i in range(n)
        ]
        await asyncio.gather(*tasks)

        self.series_data = results

    def _ignored_seasons_continue(self) -> bool:
        """After scraping ignored-season anime, check for changes and prompt.

        Returns True to continue scraping, False to stop.
        """
        has_stale = bool(self._stale_ignored_warnings)
        has_new_ep0 = any(
            f.get("reason") == "episode_0_placeholder" for f in self.failed_links
        )

        if not has_stale and not has_new_ep0:
            print("✓ Ignored seasons: all OK")
            return True

        if has_stale:
            print(
                "\n⚠ Episode 0 no longer exists for these ignored seasons"
                " — consider removing from .ignored_seasons.json:"
            )
            for w in self._stale_ignored_warnings:
                print(f"  • {w['title']} (season {w['season']}, slug: {w['slug']})")

        if has_new_ep0:
            new_ep0 = [f for f in self.failed_links if f.get("reason") == "episode_0_placeholder"]
            print(f"\n⚠ New episode 0 detected in {len(new_ep0)} anime (added to .failed_series.json):")
            for f in new_ep0:
                print(f"  • {f.get('title', f.get('url', '?'))}")

        answer = input("\nContinue scraping remaining anime? (y/n): ").strip().lower()
        if answer != 'y':
            print("✗ Scraping stopped. Saving progress...")
            self.save_checkpoint(include_data=True)
            if self.failed_links:
                self.save_failed_series()
            return False
        return True

    async def _async_run(self, single_url=None, url_list=None,
                         new_only=False, retry_failed=False,
                         account_source=None):
        """Async core of run()."""
        # Use a temp client for discovery, then close it
        tmp = await self._create_logged_in_client()
        try:
            print("✓ Logged in to aniworld.to")
            await self._async_run_inner(tmp, single_url=single_url,
                                        url_list=url_list, new_only=new_only,
                                        retry_failed=retry_failed,
                                        account_source=account_source)
        finally:
            if not tmp.is_closed:
                await tmp.aclose()

    async def _async_run_inner(self, tmp, single_url=None, url_list=None,
                               new_only=False, retry_failed=False,
                               account_source=None):

        if single_url:
            self._checkpoint_mode = 'single'
            main_url = self.normalize_to_series_url(single_url)
            m = _ANIME_PATH_RE.search(main_url)
            link = m.group(1) if m else main_url
            info = {"title": main_url.split("/")[-1], "link": link, "url": main_url}
            print(f"→ Scraping single anime: {main_url}")
            result = await self._scrape_one_series(tmp, info)
            await tmp.aclose()
            if result["title"].startswith("[ERROR"):
                self.failed_links.append(info)
            self.series_data = [result]
            return

        if url_list:
            self._checkpoint_mode = 'batch'
            series_list = []
            for u in url_list:
                main_url = self.normalize_to_series_url(u)
                m = _ANIME_PATH_RE.search(main_url)
                link = m.group(1) if m else main_url
                series_list.append({"title": main_url.split("/")[-1], "link": link, "url": main_url})
            await tmp.aclose()
            n = NUM_WORKERS if self._use_parallel and len(series_list) > 1 else 1
            await self._scrape_list(series_list, num_workers=n)
            print(f"  Successfully scraped: {len(self.series_data)}/{len(url_list)} anime")
            return

        if retry_failed:
            self._checkpoint_mode = 'retry'
            failed_list = self.load_failed_series()
            await tmp.aclose()
            if not failed_list:
                print("✓ No failed anime found")
                return
            print(f"✓ Found {len(failed_list)} failed anime — retrying in sequential mode")
            await self._scrape_list(failed_list, num_workers=1)
            return

        if account_source:
            self._checkpoint_mode = account_source
            print(f"→ Fetching {account_source} anime from account pages...")
            account_series = await self._get_account_series(tmp, source=account_source)
            self.all_discovered_series = account_series
            await tmp.aclose()
            if not account_series:
                print("✓ No anime found on account pages")
                return

            # New anime detection
            existing_slugs = self.load_existing_slugs()
            new_titles = [s["title"] for s in account_series
                          if self.get_series_slug_from_url(s.get('link', '')) not in existing_slugs]
            if new_titles:
                print(f"\nℹ {len(new_titles)} new anime detected:")
                for t in new_titles[:10]:
                    print(f"  + {t}")
                if len(new_titles) > 10:
                    print(f"  ... and {len(new_titles) - 10} more")
                print()

            # Two-phase scraping: ignored-season anime first
            ignored_slugs_set = {slug for slug, _ in self._get_ignored_seasons()}
            ignored_batch = [s for s in account_series
                             if self.get_series_slug_from_url(
                                 s.get('link', '')) in ignored_slugs_set]
            rest_batch = [s for s in account_series
                          if self.get_series_slug_from_url(
                              s.get('link', '')) not in ignored_slugs_set]

            if ignored_batch:
                print(f"→ Phase 1: Scraping {len(ignored_batch)} anime with ignored seasons...")
                await self._scrape_list(ignored_batch, num_workers=1)
                if not self._ignored_seasons_continue():
                    return

            print(f"→ Found {len(rest_batch)} remaining anime — scraping...")
            n = NUM_WORKERS if self._use_parallel else 1
            await self._scrape_list(rest_batch, num_workers=n)
            return

        if new_only:
            self._checkpoint_mode = 'new_only'
            print("→ Fetching anime list...")
            all_series = await self._get_all_series(tmp)
            await tmp.aclose()
            self.all_discovered_series = all_series
            existing_slugs = self.load_existing_slugs()
            ignored_slugs = self.get_ignored_slugs()
            new_list = [s for s in all_series
                        if self.get_series_slug_from_url(s.get('link', '')) not in existing_slugs
                        and self.get_series_slug_from_url(s.get('link', '')) not in ignored_slugs]
            print(f"→ New anime to scrape: {len(new_list)} (out of {len(all_series)})")
            if not new_list:
                print("✓ No new anime detected — nothing to scrape")
                return
            if len(new_list) <= 50:
                for s in new_list:
                    print(f"  + {s['title']}")
            await self._scrape_list(new_list, num_workers=1)
            return

        # Default: scrape all
        self._checkpoint_mode = 'all_series'
        print("→ Fetching anime list...")
        all_series = await self._get_all_series(tmp)
        await tmp.aclose()
        self.all_discovered_series = all_series
        ignored_slugs = self.get_ignored_slugs()
        print(f"✓ Found {len(all_series)} anime")

        # New anime detection
        existing_slugs = self.load_existing_slugs()
        new_titles = [s["title"] for s in all_series
                      if self.get_series_slug_from_url(s.get('link', '')) not in existing_slugs
                      and self.get_series_slug_from_url(s.get('link', '')) not in ignored_slugs]
        if new_titles:
            print(f"\nℹ {len(new_titles)} new anime detected:")
            for t in new_titles[:10]:
                print(f"  + {t}")
            if len(new_titles) > 10:
                print(f"  ... and {len(new_titles) - 10} more")
            print()

        if ignored_slugs:
            all_series = [s for s in all_series
                          if self.get_series_slug_from_url(s.get('link', '')) not in ignored_slugs]
            skipped = len(self.all_discovered_series) - len(all_series)
            if skipped:
                print(f"  Skipping {skipped} ignored anime")

        # Two-phase scraping: ignored-season anime first
        ignored_slugs_set = {slug for slug, _ in self._get_ignored_seasons()}
        ignored_batch = [s for s in all_series
                         if self.get_series_slug_from_url(
                             s.get('link', '')) in ignored_slugs_set]
        rest_batch = [s for s in all_series
                      if self.get_series_slug_from_url(
                          s.get('link', '')) not in ignored_slugs_set]

        if ignored_batch:
            print(f"→ Phase 1: Scraping {len(ignored_batch)} anime with ignored seasons...")
            await self._scrape_list(ignored_batch, num_workers=1)
            if not self._ignored_seasons_continue():
                return

        n = NUM_WORKERS if self._use_parallel else 1
        await self._scrape_list(rest_batch, num_workers=n)
        print(f"\n✓ Successfully scraped {len(self.series_data)} anime")

    # ── Public API ───────────────────────────────────────────────────────────

    def run(self, single_url=None, url_list=None, new_only=False,
            resume_only=False, retry_failed=False, parallel=None,
            account_source=None):
        """Main entry point: login, scrape, save checkpoint."""
        if parallel is not None:
            self._use_parallel = parallel
            print(f"→ Using {'multi-session' if parallel else 'single-session'} mode")
        else:
            self._use_parallel = True

        # Clear any stale pause file from a previous run
        self._clear_pause_file()

        try:
            if resume_only:
                if self.load_checkpoint():
                    print(f"→ Resuming from checkpoint ({len(self.completed_links)} anime already done)")
                else:
                    print("⚠ No checkpoint found. Starting fresh...")

            asyncio.run(self._async_run(
                single_url=single_url,
                url_list=url_list,
                new_only=new_only,
                retry_failed=retry_failed,
                account_source=account_source,
            ))

            # Alert for empty anime (0 episodes)
            empty = [s for s in self.series_data if s.get('total_episodes', 0) == 0]
            if empty:
                print(f"\n⚠ {len(empty)} anime with 0 episodes:")
                for s in empty:
                    print(f"  • {s['title']} → {s['url']}")

            self.save_checkpoint(include_data=True)
            if not self.failed_links:
                self.clear_failed_series()
            else:
                self.save_failed_series()

        except ScrapingPaused:
            self.paused = True
            self._clear_pause_file()
            self.save_checkpoint(include_data=True)
            if self.failed_links:
                self.save_failed_series()
        except BaseException:
            self.save_checkpoint(include_data=True)
            if self.failed_links:
                self.save_failed_series()
            raise
