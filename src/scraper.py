"""
AniWorld.to Anime Scraper — powered by httpx (no browser needed).
"""

import asyncio
import contextlib
import difflib
import json
import logging
import os
import re
import signal
import sys
import threading
import time
from urllib.parse import urlparse

import httpx
import lxml.etree
import lxml.html
from bs4 import BeautifulSoup

from config.config import (
    CHECKPOINT_EVERY,
    DATA_DIR,
    EMAIL,
    HTTP_REQUEST_TIMEOUT,
    NUM_WORKERS,
    PASSWORD,
    SEASON_CONCURRENCY,
    SERIES_INDEX_FILE,
    SITE_URL,
    SITE_URLS,
)
from src.atomic_io import atomic_write_json

logger = logging.getLogger(__name__)


# ── HTML parsing ────────────────────────────────────────────────────────────
# lxml parses these pages roughly 1.4x faster than the stdlib parser, and the
# golden fixtures in tests/ confirm it produces identical output on every
# captured real page. The fallback keeps the scraper working on a machine
# where lxml was never installed, at the old speed.
try:
    import lxml  # noqa: F401

    _HTML_PARSER = "lxml"
except ImportError:  # pragma: no cover - depends on the install
    _HTML_PARSER = "html.parser"


def make_soup(html: str) -> BeautifulSoup:
    """Parse a page with the fastest parser available.

    Every parse in this module goes through here so the choice is made in
    exactly one place -- a parser swap must never be able to apply to some
    pages and not others.
    """
    return BeautifulSoup(html, _HTML_PARSER)


# ── Transient-failure handling ──────────────────────────────────────────────
# Status codes worth a second attempt. A 404 is a real answer about a real
# series and must never be retried; these are the site saying "not now".
_RETRY_STATUS = frozenset({429, 500, 502, 503, 504})
_MAX_ATTEMPTS = 3
# How many times one run may recover from a mid-run session expiry.
_MAX_RELOGINS = 3
# A catalogue this much smaller than the local index is treated as suspect
# and reported to the user before anything is called vanished.
_CATALOGUE_WARN_RATIO = 0.95
_CATALOGUE_MIN_INDEX = 20
_BACKOFF_BASE = 0.5


class RateGuard:
    """Shared brake every worker checks before it issues a request.

    Raising concurrency is only safe if the scraper notices when the site
    pushes back. When any worker sees a 429/503, it parks the whole pool for
    a moment rather than letting the other workers keep hammering; the pool
    then eases back to full speed on its own. One instance is shared by all
    workers, so the slowdown is global, not per-connection.
    """

    def __init__(self) -> None:
        self._resume_at = 0.0
        self._penalty = 0.0

    async def wait(self) -> None:
        """Block until the pool is allowed to send again."""
        delay = self._resume_at - time.monotonic()
        if delay > 0:
            await asyncio.sleep(delay)

    def penalise(self, retry_after: float | None = None) -> float:
        """Record push-back from the site and park the pool.

        Doubles on repeated push-back so a site that is genuinely
        overloaded gets progressively more room, capped so a run can never
        stall indefinitely.
        """
        if retry_after and retry_after > 0:
            pause = min(retry_after, 60.0)
        else:
            self._penalty = min(max(self._penalty * 2, 1.0), 30.0)
            pause = self._penalty
        self._resume_at = max(self._resume_at, time.monotonic() + pause)
        logger.warning("Site pushed back — pausing all workers for %.1fs", pause)
        return pause

    def reward(self) -> None:
        """A clean response: decay the accumulated penalty."""
        if self._penalty:
            self._penalty = max(0.0, self._penalty * 0.5)


# NOT restricted with a SoupStrainer, deliberately. Straining season pages to
# the <table> subtree parses 1.2-1.6x faster and was byte-identical on every
# captured fixture -- but _parse_episodes accepts `.episode-table` on *any*
# element and carries generic fallbacks precisely so a site redesign does not
# break it. A strainer keyed on tag names throws that resilience away: the
# existing empty-season test uses <div class="episode-table">, guarding four
# real series (alaska-eisige-tradition s2, die-schluempfe s0,
# helden-der-baustelle s3, marry-my-husband s0), and it fails under the
# strainer. A few percent of parse time is not worth narrowing what the
# scraper can still read correctly.


def parse_season_html(html: str):
    """Parse one season page and return plain data, never a soup object.

    Deliberately called inline, not through asyncio.to_thread. Offloading it
    was tried and measured 2-2.7x SLOWER on the captured fixture pages
    (53 -> 25/22/20 pages/s at 4/8/16 concurrent), and slower still with more
    workers. lxml releases the GIL for the raw parse, but BeautifulSoup then
    builds its own object tree in pure Python and holds the GIL for most of
    the work, so threads buy contention and dispatch overhead and no
    parallelism. Keep this on the event loop unless a profile says otherwise.
    """
    return _parse_episodes(html)


class PhaseProfiler:
    """Records where a run's wall time actually goes, per phase.

    Built because guessing was wrong three times over: offloading parsing to
    threads, restricting the parsed subtree, and raising the worker count all
    looked like clear wins on paper and all measured flat or worse. Numbers
    settle those arguments in one run instead of an afternoon.

    Off unless ANIWORLD_PROFILE=1 is set, and the accounting is deliberately
    crude -- wall time per phase summed across workers, so the total exceeds
    the run's real duration whenever work overlaps. The useful figure is the
    *ratio* between phases, which tells you which one is worth attacking.
    """

    def __init__(self, enabled: bool) -> None:
        self.enabled = enabled
        self._totals: dict[str, float] = {}
        self._counts: dict[str, int] = {}

    @contextlib.contextmanager
    def phase(self, name: str):
        if not self.enabled:
            yield
            return
        start = time.perf_counter()
        try:
            yield
        finally:
            elapsed = time.perf_counter() - start
            self._totals[name] = self._totals.get(name, 0.0) + elapsed
            self._counts[name] = self._counts.get(name, 0) + 1

    def report(self, wall: float | None = None) -> None:
        if not self.enabled or not self._totals:
            return
        total = sum(self._totals.values())
        lines = ["", "Phase profile (summed across workers; overlaps, so > wall time)"]
        if wall:
            lines.append(f"  wall clock: {wall:.1f}s")
        for name, seconds in sorted(self._totals.items(), key=lambda kv: -kv[1]):
            count = self._counts[name]
            share = seconds / total * 100 if total else 0
            lines.append(
                f"  {name:<12} {seconds:8.1f}s  {share:5.1f}%  "
                f"n={count:<6} avg={seconds / count * 1000:7.1f}ms"
            )
        print("\n".join(lines))


class ProgressWriter:
    """Batches progress lines into one write instead of one write per series.

    Every console write is a synchronous syscall made from the event-loop
    thread, so with a dozen workers finishing series quickly the progress
    output itself competes with the scraping for the loop. Buffering turns a
    burst of lines into a single write.

    Nothing is dropped -- thinning the output would cost the operator the
    record of which series were processed, which is the opposite of the
    point. Lines are held for at most `interval` seconds, and `flush()` is
    called on every exit path so an interrupt cannot swallow the tail.
    """

    def __init__(self, interval: float = 0.25) -> None:
        self.interval = interval
        self._buf: list[str] = []
        self._last_flush = time.monotonic()

    def write(self, line: str) -> None:
        self._buf.append(line + "\n")
        if time.monotonic() - self._last_flush >= self.interval:
            self.flush()

    def flush(self) -> None:
        if not self._buf:
            return
        payload = "".join(self._buf)
        self._buf.clear()
        self._last_flush = time.monotonic()
        try:
            sys.stdout.write(payload)
            sys.stdout.flush()
        except Exception:  # noqa: BLE001 - a dead stdout must never kill a scrape
            logger.debug("Could not write progress output")


def _retry_after_seconds(resp) -> float | None:
    """Parse a Retry-After header, if the site sent a usable one."""
    raw = resp.headers.get("Retry-After", "")
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None



# ── Rename matching helpers ─────────────────────────────────────────────────

_STOPWORDS = frozenset(
    {
        "the",
        "a",
        "an",
        "and",
        "or",
        "of",
        "to",
        "in",
        "on",
        "at",
        "from",
        "with",
        "by",
        "no",
        "san",
        "chan",
        "kun",
        "sama",
    }
)


def _normalize_title_key(title: str) -> str:
    """Return a normalized title string for matching purposes."""
    if not title:
        return ""
    lowered = title.lower()
    lowered = re.sub(r"\(\d{4}\)", " ", lowered)
    lowered = re.sub(r"[^a-z0-9\s]", " ", lowered)
    tokens = [t for t in lowered.split() if t and t not in _STOPWORDS]
    return " ".join(sorted(set(tokens)))


def _slug_tokens(url_or_slug: str) -> str:
    """Return sorted significant tokens from a series slug or URL."""
    if not url_or_slug:
        return ""
    slug = url_or_slug.lower()
    slug = re.sub(r"https?://[^/]+", "", slug)
    slug = re.sub(r"/(?:anime/stream|serie)/", "", slug)
    slug = re.sub(r"[^a-z0-9\-]", " ", slug)
    tokens = sorted({t for t in slug.split("-") if len(t) > 2})
    return " ".join(tokens)


def _match_keys(title: str, url_or_slug: str = "") -> set[str]:
    """Return normalized match keys for a title + optional slug."""
    keys: set[str] = set()
    full = _normalize_title_key(title)
    if full:
        keys.add(full)
    slug_tokens = _slug_tokens(url_or_slug)
    if slug_tokens:
        keys.add(slug_tokens)
    return keys - {""}


def _score_rename_match(v_title: str, v_url: str, n_title: str, n_url: str) -> float:
    """Score how likely a new series is a rename of a vanished one."""
    v_keys = _match_keys(v_title, v_url)
    n_keys = _match_keys(n_title, n_url)
    if not v_keys or not n_keys:
        return 0.0
    best = 0.0
    for v_key in v_keys:
        for n_key in n_keys:
            if v_key == n_key:
                return 1.0
            v_tokens = set(v_key.split())
            n_tokens = set(n_key.split())
            if v_tokens and n_tokens:
                overlap = len(v_tokens & n_tokens) / max(len(v_tokens), len(n_tokens))
                best = max(best, overlap)
            best = max(best, difflib.SequenceMatcher(None, v_key, n_key).ratio())
    return best


def _find_vanished_renames(vanished_entries, new_entries, threshold: float = 0.75) -> set[str]:
    """Return set of new entry titles that look like renames of vanished ones.

    Advisory only -- the caller reports these, it never drops a series from
    a scrape on the strength of a guess. The threshold sits high because the
    scoring is fuzzy: at 0.35 "One Piece"/"One Punch Man" scored 0.55,
    "Death Note"/"Deadman Wonderland" 0.43 and "Bleach"/"Beelzebub" 0.40,
    so unrelated shows were being announced as renames of each other.
    """
    renames: set[str] = set()
    used_new: set[int] = set()
    new_list = list(new_entries)
    for v_title, v_url in vanished_entries:
        best_idx = -1
        best_score = 0.0
        for idx, n in enumerate(new_list):
            if idx in used_new:
                continue
            n_title = n.get("title", "")
            n_url = n.get("url", n.get("link", ""))
            score = _score_rename_match(v_title, v_url, n_title, n_url)
            if score > best_score:
                best_score = score
                best_idx = idx
        if best_idx >= 0 and best_score >= threshold:
            used_new.add(best_idx)
            renames.add(new_list[best_idx].get("title", ""))
    return renames - {""}


# ── Constants ───────────────────────────────────────────────────────────────
LOGIN_URL = f"{SITE_URL}/login"
SERIES_LIST_URL = f"{SITE_URL}/animes"
ACCOUNT_SUBSCRIBED_URL = f"{SITE_URL}/account/subscribed"
ACCOUNT_WATCHLIST_URL = f"{SITE_URL}/account/watchlist"
REQUEST_TIMEOUT = HTTP_REQUEST_TIMEOUT
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0"

_ANIME_PATH_RE = re.compile(r"(/anime/stream/[^/]+)")
_ANIME_SLUG_RE = re.compile(r"^/anime/stream/([^/?#]+)/?$")
_STAFFEL_RE = re.compile(r"/staffel-(\d+)")
_EPISODE_LABEL_RE = re.compile(r"\s*\[Episode\s+\d+\]\s*$", re.IGNORECASE)

# Error page detection — matches titles like "404", "Error 404", "502 Bad Gateway"
_ERROR_TITLE_RE = re.compile(
    r"^(?:Error\s+)?(?P<code>\d{3})\b|\b(?:Error|Fehler)\s+(?P<code2>\d{3})\b",
    re.IGNORECASE,
)
_SERVER_ERROR_CODES = {"429", "500", "502", "503", "504"}

# Generic listing-page titles that aniworld.to returns instead of a real 404
_UTILITY_PAGES = {
    "alle animes",
    "beliebte animes",
    "neue animes",
    "katalog",
}

# Language flag identifier → normalized language code
_LANGUAGE_FLAG_MAP = {
    # German / German-dubbed
    "german": "german_dub",
    "german.svg": "german_dub",
    "deutsch": "german_dub",
    "de": "german_dub",
    "ger": "german_dub",
    "deu": "german_dub",
    # English / English-dubbed
    "english": "english_dub",
    "englisch": "english_dub",
    "en": "english_dub",
    "eng": "english_dub",
    "us": "english_dub",
    "uk": "english_dub",
    # Japanese (raw)
    "japanese": "japanese_sub",
    "japanisch": "japanese_sub",
    "ja": "japanese_sub",
    "jap": "japanese_sub",
    # Common subtitled combinations
    "japanese-german": "german_sub",
    "japanese-german.svg": "german_sub",
    "japanese-english": "english_sub",
    "japanese-english.svg": "english_sub",
    "sub-german": "german_sub",
    "sub-english": "english_sub",
    "sub-de": "german_sub",
    "sub-en": "english_sub",
    "ger-sub": "german_sub",
    "eng-sub": "english_sub",
}


def _is_logged_in(soup: BeautifulSoup) -> bool:
    """Check if the page indicates a logged-in session."""
    return soup.select_one("div.avatar a[href*='/user/profil/']") is not None


def _check_error_page(soup: BeautifulSoup) -> str | None:
    """Detect HTTP error pages (404, 502, etc.) returned as HTML.

    Returns an error string like '404' if an error page is detected, None otherwise.
    """
    # If the page has series content (season nav), it's a real series page
    if soup.select_one("#stream ul li a[href*='/staffel-']") or soup.select_one("#stream ul li a[href*='/filme']"):
        return None
    # Site-specific soft-404 banner: "Die gewünschte Serie wurde nicht gefunden
    # oder ist im Moment deaktiviert."
    alert_box = soup.select_one("div.messageAlert.danger")
    if alert_box:
        text = alert_box.get_text(strip=True).lower()
        if "nicht gefunden" in text or "deaktiviert" in text:
            return "404"
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
    # Fallback: <p> containing "nicht gefunden" (aniworld.to specific 404)
    p_tag = soup.find("p")
    if p_tag and "nicht gefunden" in p_tag.get_text(strip=True).lower():
        return "404"
    return None


# ── HTML helpers ────────────────────────────────────────────────────────────


def _has_class_xpath(name: str) -> str:
    """XPath predicate matching one whitespace-delimited class token.

    `contains(@class, 'seen')` would also match `unseen`, which is exactly
    the kind of near-miss that turns into wrong watch data rather than an
    error, so class tests always go through this.
    """
    return f"contains(concat(' ', normalize-space(@class), ' '), ' {name} ')"


_ROWS_PRIMARY = f".//table[{_has_class_xpath('seasonEpisodesList')}]//tbody//tr[@data-episode-id]"
_ROWS_NO_TBODY = f".//table[{_has_class_xpath('seasonEpisodesList')}]//tr[@data-episode-id]"
_ROWS_GENERIC = ".//tbody//tr[@data-episode-id]"
_EPISODE_TABLE = f".//table[{_has_class_xpath('seasonEpisodesList')}]"
_XP_EPISODE_NUMBER = ".//meta[@itemprop='episodeNumber']/@content"
_XP_TITLE_GER = f".//td[{_has_class_xpath('seasonEpisodeTitle')}]//a//strong"
_XP_TITLE_ENG = f".//td[{_has_class_xpath('seasonEpisodeTitle')}]//a//span"
_XP_FLAG_CELL = f".//td[{_has_class_xpath('editFunctions')}]"


def _stripped_text(el) -> str:
    """lxml equivalent of BeautifulSoup's get_text(strip=True).

    Not the same as text_content().strip(): BeautifulSoup strips *each*
    text node and joins them with nothing, so "<strong>Hello <em> World
    </em></strong>" is "HelloWorld". Joining the raw text instead would
    leave "Hello  World" and quietly change every stored episode title.
    """
    return "".join(t.strip() for t in el.itertext())


def _parse_episodes(html: str) -> list[dict] | None:
    """Parse episode rows from a season page.

    Takes the raw HTML rather than a soup: building a BeautifulSoup tree
    measured ~73% of this function's cost, and the event loop was spending
    30-55% of a run's wall clock inside it (workers share one loop, so that
    time is not overlapped with anything). Going straight to lxml is 5.8x
    faster on real season pages and lets the pool actually use the workers
    it has. Output is byte-identical: verified against the recorded output
    of the previous implementation over 535 real pages and 11,951 episodes.

    Uses aniworld.to-specific selectors:
      - Table: table.seasonEpisodesList tbody tr[data-episode-id]
      - Number: meta[itemprop='episodeNumber'] content attr
      - Title (DE): td.seasonEpisodeTitle a strong
      - Title (EN): td.seasonEpisodeTitle a span (strip [Episode N] suffix)
      - Watched: 'seen' class on the row
      - Languages: td.editFunctions img.flag
    Falls back to generic selectors if primary ones fail.

    Returns:
        list[dict]: Parsed episodes. An empty list means the episode table
        was found but genuinely holds no rows -- a season listed in the nav
        before its episodes are uploaded. That is a real state (observed on
        the sibling s.to index) and must be stored as 0, not an error.
        None: the episode table could not be located at all, or a row was
        found whose episode number could not be determined. Callers must
        treat None as a scrape failure -- storing 0 there corrupts the index
        and shows up later as a false "lost all its episodes" mismatch.
    """
    try:
        doc = lxml.html.fromstring(html)
    except (lxml.etree.ParserError, ValueError):
        # An empty or non-markup body is a failed fetch, not an empty season.
        return None

    rows = doc.xpath(_ROWS_PRIMARY) or doc.xpath(_ROWS_NO_TBODY) or doc.xpath(_ROWS_GENERIC)
    if not rows:
        # Tell "the table is there, it's just empty" (a real, if rare,
        # season state) apart from "this page has no episode table at all"
        # (a redesign, a truncated response, or a soft error page --
        # aniworld.to serves a nav-less generic page for a season that
        # doesn't exist, which lands here).
        return [] if doc.xpath(_EPISODE_TABLE) else None

    episodes = []
    for idx, row in enumerate(rows, start=1):
        # Episode number from meta tag
        found = row.xpath(_XP_EPISODE_NUMBER)
        ep_num = found[0] if found else ""
        if not ep_num:
            # Filme/movie pages lack meta episodeNumber — fall back to
            # data-episode-season-id
            ep_num = row.get("data-episode-season-id", "")
        if not ep_num:
            logger.warning("Could not determine episode number for row %d", idx)
            return None

        try:
            ep_num_int = int(str(ep_num))
        except ValueError:
            logger.warning("Non-numeric episode number '%s' in row %d", ep_num, idx)
            return None

        # German title
        ger_el = row.xpath(_XP_TITLE_GER)
        title_ger = _stripped_text(ger_el[0]) if ger_el else ""

        # English title (strip [Episode NNN] suffix)
        eng_el = row.xpath(_XP_TITLE_ENG)
        title_eng = _stripped_text(eng_el[0]) if eng_el else ""
        title_eng = _EPISODE_LABEL_RE.sub("", title_eng).strip()

        # Watched status from row class
        watched = "seen" in (row.get("class") or "").split()

        # Language flags (img.flag in td.editFunctions, plus SVG fallbacks)
        languages: list[str] = []
        seen: set[str] = set()

        flag_cells = row.xpath(_XP_FLAG_CELL)
        if flag_cells:
            flag_cell = flag_cells[0]
            # IMG src style
            for img in flag_cell.iter("img"):
                src = str(img.get("src", ""))
                title_attr = str(img.get("title", ""))
                alt_attr = str(img.get("alt", ""))
                flag_file = src.rsplit("/", 1)[-1] if "/" in src else src

                # Try full filename first, then stem
                code = _LANGUAGE_FLAG_MAP.get(flag_file.lower())
                if not code:
                    stem = flag_file.rsplit(".", 1)[0] if "." in flag_file else flag_file
                    code = _LANGUAGE_FLAG_MAP.get(stem.lower())
                if not code:
                    code = _LANGUAGE_FLAG_MAP.get(title_attr.lower())
                if not code:
                    code = _LANGUAGE_FLAG_MAP.get(alt_attr.lower())

                if code and code not in seen:
                    seen.add(code)
                    languages.append(code)

            # SVG <use href="#icon-flag-..." style
            for use in flag_cell.iter("use"):
                href = str(use.get("href") or use.get("xlink:href") or "")
                m = re.search(r"icon-flag-([a-z0-9\-]+)", href, re.IGNORECASE)
                if m:
                    code = _LANGUAGE_FLAG_MAP.get(m.group(1).lower())
                    if code and code not in seen:
                        seen.add(code)
                        languages.append(code)

            # SVG class style
            for svg in flag_cell.iter("svg"):
                classes = svg.get("class") or ""
                for token in re.findall(r"flag-([a-z0-9\-]+)", classes, re.IGNORECASE):
                    code = _LANGUAGE_FLAG_MAP.get(token.lower())
                    if code and code not in seen:
                        seen.add(code)
                        languages.append(code)

        ep = {"number": ep_num_int, "watched": watched}
        if title_ger:
            ep["title_ger"] = title_ger
        if title_eng:
            ep["title_eng"] = title_eng
        if languages:
            ep["languages"] = languages
        episodes.append(ep)
    return episodes

def _extract_season_links(soup: BeautifulSoup, series_slug: str, base_url: str | None = None) -> list[tuple[str, str]]:
    """Extract season numbers and URLs from the #stream season navigation.

    Handles staffel-N seasons and Filme (movies/specials).
    """
    if base_url is None:
        base_url = SITE_URL
    seasons = []
    seen = set()

    # Primary: href pattern /anime/stream/{slug}/staffel-{num}
    for a_tag in soup.select("#stream ul:first-of-type li a[href*='/staffel-']"):
        href = str(a_tag.get("href", ""))
        m = _STAFFEL_RE.search(href)
        if m and m.group(1) not in seen:
            season_num = m.group(1)
            seen.add(season_num)
            url = f"{base_url}/anime/stream/{series_slug}/staffel-{season_num}"
            seasons.append((season_num, url))

    # Check for Filme (movies/OVAs/specials)
    for a_tag in soup.select("#stream ul:first-of-type li a[href*='/filme']"):
        href = str(a_tag.get("href", ""))
        if "Filme" not in seen:
            seen.add("Filme")
            url = f"{base_url}/anime/stream/{series_slug}/filme"
            seasons.append(("Filme", url))

    if seasons:
        return seasons

    # Fallback: any staffel links on the page
    staffel_pattern = re.compile(rf"/anime/stream/{re.escape(series_slug)}/staffel-(\d+)", re.IGNORECASE)
    for a_tag in soup.find_all("a", href=True):
        m = staffel_pattern.search(str(a_tag["href"]))
        if m and m.group(1) not in seen:
            seen.add(m.group(1))
            url = f"{base_url}/anime/stream/{series_slug}/staffel-{m.group(1)}"
            seasons.append((m.group(1), url))

    return seasons


def _heading_text(el) -> str:
    """Return the flattened, normalized text of a heading element.

    Uses a separator-joined get_text() so inline sub-elements (e.g. "Harry
    Potter<small>Specials</small>") don't glue themselves to the main text.
    Does not mutate the parse tree -- safe to call on a soup shared with
    other extractors.
    """
    if not el:
        return ""
    text = " ".join(el.get_text(" ", strip=True).split())
    text = re.sub(r"\s*(?:Staffel|Season|St\.?)\s*\d+.*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*Specials\s*$", "", text, flags=re.IGNORECASE)
    return text.strip()


def _extract_title(soup: BeautifulSoup) -> str | None:
    """Extract series title from the page.

    Tries site-specific heading(s) first, then falls back to generic headings.
    Strips trailing 'Staffel N' / 'Season N' / 'Specials' suffixes and
    ensures inline tags like <small> do not glue themselves to the main text.
    """
    # aniworld.to preferred headings
    for selector in ("h1[itemprop='name'] > span", "h1.fw-bold"):
        el = soup.select_one(selector)
        text = _heading_text(el)
        if text:
            return text

    # Fallback headings
    for tag in ("h2", "h1"):
        el = soup.find(tag)
        text = _heading_text(el)
        if text:
            return text
    return None


def _count_seasons_from_html(soup: BeautifulSoup) -> int:
    """Return number of distinct seasons found on an aniworld.to series page."""
    season_links = soup.select("#seasons li a")
    if not season_links:
        # Fallback: look for staffel-N or season-N links anywhere in the page.
        season_links = soup.select("a[href*='staffel-'], a[href*='season-']")
    labels = set()
    for a in season_links:
        href = str(a.get("href", ""))
        m = re.search(r"(?:^|/)(?:staffel|season)-(\d+)", href, re.IGNORECASE)
        if m:
            labels.add(int(m.group(1)))
            continue
        text = a.get_text(strip=True)
        m = re.search(r"(?:staffel|season|st\.?)\s*(\d+)", text, re.IGNORECASE)
        if m:
            labels.add(int(m.group(1)))
    return len(labels)


def _extract_alt_titles(soup: BeautifulSoup) -> list[str]:
    """Extract alternative titles from the series detail page."""
    alt_el = soup.select_one("h1[itemprop='name']")
    if alt_el:
        alt_raw = str(alt_el.get("data-alternativetitles", ""))
        if alt_raw:
            return [t.strip() for t in alt_raw.split(",") if t.strip()]
    return []


def _detect_subscription_status(soup: BeautifulSoup) -> tuple[bool | None, bool | None]:
    """Detect subscription and watchlist status from a series page.

    Uses aniworld.to's div.add-series container with data-series-favourite
    and data-series-watchlist attributes (value "1" = active).
    Cross-validates with CSS classes li.setFavourite.true / li.setWatchlist.true.

    Returns (subscribed, watchlist) — None if container not found or not logged in.
    """
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
        logger.warning(
            "Subscribe mismatch: data-attr=%s, CSS=%s — trusting data attribute",
            subscribed,
            css_subscribed,
        )
    if watchlist is not None and css_watchlist != watchlist:
        logger.warning(
            "Watchlist mismatch: data-attr=%s, CSS=%s — trusting data attribute",
            watchlist,
            css_watchlist,
        )

    # Fallback to CSS if data attributes missing
    if subscribed is None:
        subscribed = css_subscribed
    if watchlist is None:
        watchlist = css_watchlist

    return (subscribed, watchlist)


# ── Exception ───────────────────────────────────────────────────────────────


class ScrapingPausedError(Exception):
    pass


# ── AniWorldScraper (httpx) ────────────────────────────────────────────────


class AniWorldScraper:
    """AniWorld.to anime scraper powered by httpx (no browser needed)."""

    def __init__(self):
        self.series_data: list[dict] = []
        self.all_discovered_series: list[dict] | None = None
        self.completed_links: set[str] = set()
        self.failed_links: list[dict] = []
        self.attempted_urls: set[str] = set()

        self.checkpoint_file = os.path.join(DATA_DIR, ".scrape_checkpoint.json")
        self.failed_file = os.path.join(DATA_DIR, ".failed_series.json")
        self.ignore_file = os.path.join(DATA_DIR, ".ignored_series.json")
        self.ignored_seasons_file = os.path.join(DATA_DIR, ".ignored_seasons.json")
        self.pause_file = os.path.join(DATA_DIR, ".pause_scraping")

        self._checkpoint_mode: str | None = None
        self._use_parallel: bool = True
        self._lock = threading.Lock()
        self._relogin_count = 0
        self._last_pause_check = 0.0
        self._pause_cached = False
        self.paused = False
        self._ignored_seasons_cache: set[tuple[str, str]] | None = None
        self._stale_ignored_warnings: list[dict] = []
        self.site_url = SITE_URL
        self._interrupt_requested = False
        self._rate_guard = RateGuard()
        self._progress = ProgressWriter()
        self._shared_client = None
        self._client_users = 0
        self._client_lock = asyncio.Lock()
        # Connection pool is sized from this, not from NUM_WORKERS directly,
        # so a run that overrides the worker count (a benchmark, a scoped
        # run) gets a pool that matches it. A pool smaller than the actual
        # fan-out silently serialises the requests it cannot carry.
        self.pool_workers = NUM_WORKERS
        self._profiler = PhaseProfiler(os.getenv("ANIWORLD_PROFILE", "") == "1")

    # ── Static / class methods ──────────────────────────────────────────────

    @staticmethod
    def get_checkpoint_mode(data_dir):
        cp_file = os.path.join(data_dir, ".scrape_checkpoint.json")
        try:
            if os.path.exists(cp_file):
                with open(cp_file, encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    return data.get("mode")
        except Exception:
            pass
        return None

    # ── Checkpoint management ───────────────────────────────────────────────

    def _sync_save_checkpoint(self, include_data=False):
        """Synchronous checkpoint writer; thread-safe."""
        with self._lock:
            payload = {
                "completed_links": list(self.completed_links),
                "mode": self._checkpoint_mode,
                "timestamp": time.time(),
            }
            if include_data:
                payload["series_data"] = self.series_data
            try:
                # No backup here (unlike failed/ignore lists): this is the
                # highest-frequency writer -- it can include the full
                # series_data -- and it's precisely the recovery file for
                # an unclean shutdown, so durability (fsync) matters far
                # more than keeping a history of it.
                atomic_write_json(self.checkpoint_file, payload, indent=None, backup=False)
            except Exception as e:
                logger.error("Failed to save checkpoint: %s", e)

    def save_checkpoint(self, include_data=False):
        """Synchronous entry point for final/pause/error paths."""
        self._sync_save_checkpoint(include_data=include_data)

    async def asave_checkpoint(self, include_data=False):
        """Offload checkpoint I/O to a thread so the event loop stays free."""
        with self._profiler.phase("checkpoint"):
            await asyncio.to_thread(self._sync_save_checkpoint, include_data)

    def load_checkpoint(self) -> bool:
        with self._lock:
            try:
                if not os.path.exists(self.checkpoint_file):
                    return False
                with open(self.checkpoint_file, encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    self.completed_links = set(data.get("completed_links", []))
                    self._checkpoint_mode = data.get("mode")
                    saved_data = data.get("series_data")
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

    def _write_failed_entries(self, entries: list):
        """Write entries to the failed-series file, or remove it if empty.

        Caller must hold self._lock.
        """
        if not entries:
            try:
                if os.path.exists(self.failed_file):
                    os.remove(self.failed_file)
            except OSError:
                pass
            return
        try:
            atomic_write_json(self.failed_file, entries, backup=False)
        except Exception as e:
            logger.error("Failed to save failed series: %s", e)

    def save_failed_series(self, replace=False):
        """Persist failed series list to disk.

        If replace=True (retry mode), overwrites the file with only the
        current failed_links -- removing series that succeeded this run.
        If replace=False (default), merges new failures into the existing list.

        Note: replace=True is only safe when this run's scope covers every
        entry already on disk (a true full-catalog retry). For scoped runs
        (single URL, a specific url_list, etc.) use reconcile_failed_series()
        instead, which only touches entries this run actually attempted.
        """
        with self._lock:
            # Filter out ignored series
            ignored_urls = {e.get("url", "") for e in self.load_ignored_series()}

            if replace:
                to_save = [
                    item for item in self.failed_links if isinstance(item, dict) and item.get("url") not in ignored_urls
                ]
            else:
                existing = []
                try:
                    if os.path.exists(self.failed_file):
                        with open(self.failed_file, encoding="utf-8") as f:
                            existing = json.load(f)
                except Exception:
                    pass
                existing = [e for e in existing if isinstance(e, dict) and e.get("url") not in ignored_urls]
                seen = {e.get("url") for e in existing if isinstance(e, dict)}
                for item in self.failed_links:
                    if isinstance(item, dict) and item.get("url") not in seen and item.get("url") not in ignored_urls:
                        existing.append(item)
                        seen.add(item.get("url"))
                to_save = existing
            self._write_failed_entries(to_save)

    def reconcile_failed_series(self):
        """Reconcile the persisted failed-series file with this run's outcome.

        Drops entries for URLs this run actually attempted (they either
        succeeded, in which case they no longer belong on the list, or
        failed again, in which case they get re-added from failed_links
        below) and leaves every other entry untouched. This matters for
        scoped runs (single URL, a specific url_list, retry-only) that
        cover a small subset of series -- e.g. the "critical series"
        integrity rescrape uses a freshly-constructed AniWorldScraper for
        just a handful of URLs, and previously relied on replace-on-empty
        logic that wiped out an unrelated full scrape's entire failed list
        just because its own handful of URLs all happened to succeed.
        """
        with self._lock:
            ignored_urls = {e.get("url", "") for e in self.load_ignored_series()}
            existing = []
            try:
                if os.path.exists(self.failed_file):
                    with open(self.failed_file, encoding="utf-8") as f:
                        existing = json.load(f)
            except Exception:
                pass
            kept = [
                e
                for e in existing
                if isinstance(e, dict) and e.get("url") not in self.attempted_urls and e.get("url") not in ignored_urls
            ]
            seen = {e.get("url") for e in kept}
            for e in self.failed_links:
                if isinstance(e, dict) and e.get("url") not in seen and e.get("url") not in ignored_urls:
                    kept.append(e)
                    seen.add(e.get("url"))
            self._write_failed_entries(kept)

    def load_failed_series(self) -> list:
        with self._lock:
            try:
                if os.path.exists(self.failed_file):
                    with open(self.failed_file, encoding="utf-8") as f:
                        data = json.load(f)
                    if isinstance(data, list):
                        return data
            except Exception:
                pass
            return []

    # ── Ignore list management ──────────────────────────────────────────────

    def load_ignored_series(self) -> list[dict]:
        try:
            if os.path.exists(self.ignore_file):
                with open(self.ignore_file, encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, list):
                    return data
        except Exception:
            pass
        return []

    def get_ignored_slugs(self) -> set[str]:
        return {self.get_series_slug_from_url(s.get("url", "")) for s in self.load_ignored_series()} - {"unknown"}

    async def _revalidate_ignored_series(self, client: httpx.AsyncClient):
        """Re-check ignored series to see if they are still empty/404.

        Notification-only — does not auto-remove entries.  Prints a warning
        for any series that now appears to have content so the user can
        manually remove it from .ignored_series.json.
        """
        ignored = self.load_ignored_series()
        if not ignored:
            return

        print(f"→ Re-checking {len(ignored)} ignored anime...")
        now_available = 0
        still_empty = 0

        for entry in ignored:
            url = entry.get("url", "")
            title = entry.get("title", url.split("/")[-1])
            try:
                resp = await client.get(url, follow_redirects=True)
                soup = make_soup(resp.text)
                error_code = _check_error_page(soup)
                if error_code:
                    still_empty += 1
                    print(f"  ✓ {title}: still empty ({error_code} — ignored)")
                else:
                    has_seasons = soup.select_one("#stream ul li a[href*='/staffel-']") or soup.select_one(
                        "#stream ul li a[href*='/filme']"
                    )
                    if has_seasons:
                        now_available += 1
                        print(f"  ⚠ {title}: now available! Consider removing from .ignored_series.json")
                    else:
                        still_empty += 1
                        print(f"  ✓ {title}: still empty (no seasons — ignored)")
            except httpx.HTTPError as e:
                still_empty += 1
                print(f"  ✓ {title}: still unreachable ({e} — ignored)")

        print(f"→ Re-checked {len(ignored)} ignored: {now_available} now available, {still_empty} still empty")

    def _check_ignored_vs_catalog(self, all_series: list[dict]):
        """Compare ignored series against the fetched catalog.

        Notification-only — prints which ignored series have disappeared
        from the catalog and which are still listed.
        """
        ignored = self.load_ignored_series()
        if not ignored:
            return

        catalog_slugs = {self.get_series_slug_from_url(s.get("link", "")) for s in all_series} - {"unknown"}

        in_catalog = []
        disappeared = []
        for entry in ignored:
            slug = self.get_series_slug_from_url(entry.get("url", ""))
            title = entry.get("title", slug)
            if slug in catalog_slugs:
                in_catalog.append(title)
            else:
                disappeared.append(title)

        if disappeared:
            print(
                f"\n⚠ {len(disappeared)} ignored anime no longer in catalog"
                " — consider removing from .ignored_series.json:"
            )
            for t in disappeared:
                print(f"  ✕ {t}")
        if in_catalog:
            print(f"ℹ {len(in_catalog)} ignored anime still listed in catalog (skipped)")
        print()

    def _check_index_vs_catalog(
        self,
        all_series: list[dict],
        *,
        quiet: bool = False,
    ):
        """Detect indexed series that have disappeared from the catalog.

        Notification-only by default. Set quiet=True when the caller
        will present a vanished/new comparison table later.
        """
        catalog_slugs = {self.get_series_slug_from_url(s.get("link", "")) for s in all_series} - {"unknown"}

        index_map: dict[str, str] = {}
        try:
            if os.path.exists(SERIES_INDEX_FILE):
                with open(SERIES_INDEX_FILE, encoding="utf-8") as f:
                    data = json.load(f)
                items = data if isinstance(data, list) else list(data.values())
                for item in items:
                    url = item.get("url", "") or item.get("link", "")
                    slug = self.get_series_slug_from_url(url)
                    if slug != "unknown":
                        index_map[slug] = item.get("title", slug)
        except Exception:
            return

        if not index_map:
            return

        vanished = [(slug, title) for slug, title in index_map.items() if slug not in catalog_slugs]

        if vanished and not quiet:
            print(f"\n⚠ {len(vanished)} indexed anime no longer in catalog — may have been removed from the site:")
            for slug, title in vanished[:20]:
                print(f"  ✕ {title}  ({slug})")
            if len(vanished) > 20:
                print(f"  ... and {len(vanished) - 20} more")
            print("  → Review series_index.json and decide whether to keep or remove these entries.")
            print()

        return vanished

    def _vanished_index_entries(self, all_series: list[dict]) -> list[tuple[str, str]]:
        """Return (title, url) for indexed series missing from the catalog."""
        catalog_slugs = {self.get_series_slug_from_url(s.get("link", "")) for s in all_series} - {"unknown"}
        entries: list[tuple[str, str]] = []
        try:
            if not os.path.exists(SERIES_INDEX_FILE):
                return entries
            with open(SERIES_INDEX_FILE, encoding="utf-8") as f:
                data = json.load(f)
            items = data if isinstance(data, list) else list(data.values())
            for item in items:
                if not isinstance(item, dict):
                    continue
                # Match the catalogue side, which reads "link": comparing a
                # url-derived slug against link-derived ones would disagree the
                # moment the two fields differ, and show_vanished_series also
                # prefers "link". Both are kept as fallbacks so an entry
                # carrying only one of them still resolves.
                slug_source = item.get("link", "") or item.get("url", "")
                slug = self.get_series_slug_from_url(slug_source)
                if slug and slug != "unknown" and slug not in catalog_slugs:
                    entries.append((item.get("title", slug), item.get("url", "") or slug_source))
        except Exception:
            pass
        return entries

    def _filter_new_entries(self, new_entries, all_series):
        """Return (all_entries, rename_titles) -- renames are flagged, never skipped.

        A likely rename is worth telling the user about, but acting on it means
        a genuinely new series is never scraped and so never enters the index.
        The rename score is a fuzzy guess; the index is a mirror of the site.
        Report the suspicion, scrape everything, and let the user decide.
        """
        vanished_entries = self._vanished_index_entries(all_series)
        rename_titles = _find_vanished_renames(
            [(t, u) for t, u in vanished_entries],
            new_entries,
        )
        return list(new_entries), rename_titles

    # ── Ignored seasons management ──────────────────────────────────────────

    def load_ignored_seasons(self) -> list[dict]:
        try:
            if os.path.exists(self.ignored_seasons_file):
                with open(self.ignored_seasons_file, encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, list):
                    return data
        except Exception:
            pass
        return []

    def get_ignored_seasons_set(self) -> set[tuple[str, str]]:
        """Return set of (slug, season) tuples that have episode 0 ignored.

        Slugs are normalized to lowercase so mixed-case entries in
        .ignored_seasons.json still match the lowercase slugs derived
        from canonical aniworld.to URLs.
        """
        return {(str(e.get("slug", "")).lower(), str(e.get("season", ""))) for e in self.load_ignored_seasons()} - {
            ("", "")
        }

    def _get_ignored_seasons(self) -> set[tuple[str, str]]:
        """Cached version — loads from file once per run."""
        if self._ignored_seasons_cache is None:
            self._ignored_seasons_cache = self.get_ignored_seasons_set()
        return self._ignored_seasons_cache

    # ── URL helpers ─────────────────────────────────────────────────────────

    def get_series_slug_from_url(self, url):
        try:
            path = urlparse(url).path if url.startswith("http") else url
            parts = path.split("/")
            # /anime/stream/{slug}
            if "stream" in parts:
                idx = parts.index("stream")
                if idx + 1 < len(parts) and parts[idx + 1]:
                    return parts[idx + 1]
            return "unknown"
        except Exception:
            return "unknown"

    def normalize_to_series_url(self, url):
        if not url:
            return url
        url = url.split("?")[0].split("#")[0]
        m = _ANIME_PATH_RE.search(url)
        if m:
            return f"{SITE_URL}{m.group(1)}"
        # Bare slug
        slug = url.strip().strip("/")
        if slug and not slug.startswith("http"):
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

    def _check_interrupt_flag(self):
        """Raise KeyboardInterrupt if a shutdown was requested."""
        if self._interrupt_requested:
            raise KeyboardInterrupt("Interrupt flag set")

    def _clear_pause_file(self):
        try:
            if os.path.exists(self.pause_file):
                os.remove(self.pause_file)
        except OSError:
            pass

    def _create_pause_file(self):
        """Create the pause file so workers raise ScrapingPausedError."""
        try:
            with open(self.pause_file, "w", encoding="utf-8") as f:
                f.write("PAUSE")
        except OSError:
            pass

    # ── Index helpers (for new_only mode) ───────────────────────────────────

    def _confirm_catalogue_size(self, all_series) -> bool:
        """Warn when a fetched catalogue looks too short to be genuine, and ask.

        A truncated or degraded catalogue response still parses cleanly, so a
        short list is indistinguishable from a site that really did lose
        series -- except by size. Every indexed entry missing from it is later
        reported as vanished and offered for deletion, so a bad fetch can put
        thousands of good entries in front of a delete-all prompt.

        This only reports and asks; the user decides whether to go on. It is
        deliberately not an automatic abort: a site genuinely shrinking is a
        real thing, and only the user can tell the two apart.
        """
        try:
            indexed = len(self.load_existing_slugs())
        except Exception:
            return True
        fetched = len(all_series)
        if indexed < _CATALOGUE_MIN_INDEX or fetched >= indexed * _CATALOGUE_WARN_RATIO:
            return True

        missing = indexed - fetched
        pct = (fetched / indexed * 100) if indexed else 0.0
        print("\n" + "!" * 70)
        print("  [WARN] The fetched catalogue looks unusually small.")
        print(f"    indexed series : {indexed:,}")
        print(f"    fetched now    : {fetched:,}  ({pct:.1f}% of the index)")
        print(f"    would be flagged as vanished: up to {missing:,}")
        print("  A truncated or partial response looks exactly like this.")
        print("  Continuing is fine if the site really did shrink.")
        print("!" * 70)
        logger.warning(
            "Catalogue smaller than index: fetched %d vs indexed %d (%.1f%%)",
            fetched,
            indexed,
            pct,
        )
        answer = input("\nContinue with this scrape anyway? (y/n): ").strip().lower()
        if answer != "y":
            print("  -> Scrape cancelled. The index was not touched.")
            logger.info("User cancelled scrape after short-catalogue warning.")
            return False
        return True

    def load_existing_slugs(self) -> set[str]:
        existing = set()
        try:
            if os.path.exists(SERIES_INDEX_FILE):
                with open(SERIES_INDEX_FILE, encoding="utf-8") as f:
                    data = json.load(f)
                items = data if isinstance(data, list) else list(data.values())
                for item in items:
                    url = item.get("url", "") or item.get("link", "")
                    if url:
                        existing.add(self.get_series_slug_from_url(url))
        except Exception:
            pass
        existing.discard("unknown")
        return existing

    # ── Async internals ─────────────────────────────────────────────────────

    async def _login_client(self, client: httpx.AsyncClient) -> None:
        """Log in an existing httpx client to aniworld.to."""
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
        # POST returns empty body — verify on the catalogue page before
        # returning the client, matching the other scrapers/watchmaker pattern.
        verify_resp = await client.get(SERIES_LIST_URL)
        verify_soup = make_soup(verify_resp.text)
        if not _is_logged_in(verify_soup):
            await client.aclose()
            raise RuntimeError("Login failed — check credentials")

    async def _create_logged_in_client(self) -> httpx.AsyncClient:
        client = httpx.AsyncClient(
            http2=True,
            headers={
                "User-Agent": UA,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "de,en-US;q=0.7,en;q=0.3",
                "Accept-Encoding": "gzip, deflate, br",
                "Upgrade-Insecure-Requests": "1",
            },
            timeout=httpx.Timeout(REQUEST_TIMEOUT, connect=10.0),
            follow_redirects=True,
            limits=httpx.Limits(
                # One worker now has up to SEASON_CONCURRENCY season fetches
                # in flight at once, so a 2-connection pool would serialise
                # the fan-out straight back into the queue it was meant to
                # remove. Keepalive matches it so those connections survive
                # between series instead of re-handshaking each time.
                max_connections=self.pool_workers * SEASON_CONCURRENCY + 4,
                max_keepalive_connections=self.pool_workers * SEASON_CONCURRENCY + 4,
            ),
        )
        await self._login_client(client)
        return client

    async def _get_all_series(self, client: httpx.AsyncClient) -> list[dict]:
        """Fetch the full anime catalogue from aniworld.to/animes."""
        resp = await self._get(client, SERIES_LIST_URL)
        soup = make_soup(resp.text)
        if not _is_logged_in(soup):
            raise RuntimeError("Not logged in — cannot fetch anime catalogue")
        series, seen_slugs = [], set()

        # Primary: #seriesContainer ul li a
        for a in soup.select("#seriesContainer ul li a"):
            href = str(a.get("href", ""))
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
            alt_titles_raw = str(a.get("data-alternative-title", ""))
            alt_titles = [t.strip() for t in alt_titles_raw.split(",") if t.strip()] if alt_titles_raw else []

            series.append(
                {
                    "title": title,
                    "link": f"/anime/stream/{slug}",
                    "url": f"{SITE_URL}/anime/stream/{slug}",
                    "alt_titles": alt_titles,
                }
            )

        # Fallback: scan all links if container selector failed
        if not series:
            for a in soup.find_all("a", href=True):
                href = str(a["href"])
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
                series.append(
                    {
                        "title": title,
                        "link": f"/anime/stream/{slug}",
                        "url": f"{SITE_URL}/anime/stream/{slug}",
                    }
                )

        return series

    async def _get_account_series(self, client: httpx.AsyncClient, source: str = "both") -> list[dict]:
        """Fetch subscribed/watchlist anime from account pages.

        Args:
            source: 'subscribed', 'watchlist', or 'both'

        Returns list of series dicts with title, link, url keys.
        Note: aniworld.to account pages have no pagination.
        """
        pages = []
        if source in ("subscribed", "both"):
            pages.append((ACCOUNT_SUBSCRIBED_URL, "Subscriptions"))
        if source in ("watchlist", "both"):
            pages.append((ACCOUNT_WATCHLIST_URL, "Watchlist"))

        seen_slugs = set()
        series_list = []

        for base_url, label in pages:
            count_before = len(series_list)
            try:
                resp = await client.get(base_url, follow_redirects=True)
            except httpx.HTTPError as e:
                logger.warning("Could not fetch %s: %s", base_url, e)
                continue

            soup = make_soup(resp.text)
            if not _is_logged_in(soup):
                raise RuntimeError(f"Not logged in — cannot fetch {label} page")

            # aniworld.to: div.seriesListContainer > div a with h3 for title
            for entry in soup.select("div.seriesListContainer > div a"):
                href = str(entry.get("href", ""))
                m = _ANIME_SLUG_RE.match(href)
                if not m:
                    continue
                slug = m.group(1)
                if slug in seen_slugs:
                    continue
                seen_slugs.add(slug)

                title_el = entry.select_one("h3")
                title = title_el.get_text(strip=True) if title_el else slug

                series_list.append(
                    {
                        "title": title,
                        "link": f"/anime/stream/{slug}",
                        "url": f"{SITE_URL}/anime/stream/{slug}",
                    }
                )

            # Fallback: generic link scan
            if len(series_list) == count_before:
                for link in soup.find_all("a", href=True):
                    href = str(link.get("href", ""))
                    m = _ANIME_SLUG_RE.match(href)
                    if not m:
                        continue
                    slug = m.group(1)
                    if slug in seen_slugs:
                        continue
                    seen_slugs.add(slug)
                    title = link.get_text(strip=True) or slug
                    series_list.append(
                        {
                            "title": title,
                            "link": f"/anime/stream/{slug}",
                            "url": f"{SITE_URL}/anime/stream/{slug}",
                        }
                    )

            count_after = len(series_list)
            print(f"  ✓ {label}: {count_after - count_before} anime found")

        return series_list

    async def _get(self, client, url, **kwargs):
        """GET a page, retrying only what is worth retrying.

        Before this existed a single dropped connection or one 502 failed the
        entire series: it went straight onto the failed list and its data was
        whatever the index already held until someone ran the retry menu by
        hand. Recovering in place is both faster than a second pass and more
        accurate than leaving the series stale, and it is what makes running
        more workers safe -- the pool now reacts to push-back instead of
        ignoring it.

        Raises the last httpx error if every attempt fails, so callers keep
        their existing error handling.
        """
        kwargs.setdefault("follow_redirects", True)
        delay = _BACKOFF_BASE
        last_exc: Exception | None = None
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            await self._rate_guard.wait()
            try:
                with self._profiler.phase("network"):
                    resp = await client.get(url, **kwargs)
            except httpx.HTTPError as exc:
                last_exc = exc
            else:
                if resp.status_code not in _RETRY_STATUS:
                    self._rate_guard.reward()
                    return resp
                if resp.status_code in (429, 503):
                    self._rate_guard.penalise(_retry_after_seconds(resp))
                last_exc = httpx.HTTPStatusError(
                    f"HTTP {resp.status_code}",
                    request=resp.request,
                    response=resp,
                )
            if attempt < _MAX_ATTEMPTS:
                logger.debug("Retrying %s (attempt %d/%d): %s", url, attempt, _MAX_ATTEMPTS, last_exc)
                await asyncio.sleep(delay)
                delay *= 2
        assert last_exc is not None
        raise last_exc

    async def _fetch_season_pages(self, client, season_links):
        """Fetch every season page of one series at once, in order.

        Season pages are independent GETs, but they used to be fetched one
        after another, so a series cost one round trip per season and its
        recorded scrape time scaled straight off the season count (on s.to:
        1 season 1.2s, 3 seasons 2.3s, 5 seasons 3.4s, 10 seasons 6.3s).
        Nothing in the parsing depends on the order they arrive in, only on
        the order they are *processed* in, which the caller still controls.

        The semaphore stops a 57-season series from opening 57 sockets at
        once. Results come back positionally aligned with `season_links`, a
        failed fetch as the exception object, so the caller keeps its
        original first-failure-wins behaviour exactly.
        """
        sem = asyncio.Semaphore(SEASON_CONCURRENCY)

        async def fetch(url):
            async with sem:
                resp = await self._get(client, url)
                return resp.text

        return await asyncio.gather(
            *(fetch(url) for _, url in season_links),
            return_exceptions=True,
        )

    async def _scrape_one_series(self, client: httpx.AsyncClient, info: dict) -> dict:
        """Scrape a single anime: all seasons, episodes, subscription status."""
        t_start = time.perf_counter()
        url = info.get("scrape_url", info["url"])
        slug = self.get_series_slug_from_url(info["url"])

        try:
            resp = await self._get(client, url)
        except httpx.HTTPError as e:
            return self._error_result(info, str(e))

        soup = make_soup(resp.text)

        # Detect error pages (404, 502, etc.) before parsing content
        error_code = _check_error_page(soup)
        if error_code:
            reason = f"{error_code} server error" if error_code in _SERVER_ERROR_CODES else f"{error_code} error page"
            logger.warning("Error page detected for %s: %s", url, error_code)
            return self._error_result(info, reason)

        # Verify still logged in
        if not _is_logged_in(soup):
            # One shared session serves the whole run, so an expiry here would
            # otherwise fail every remaining series. Re-login once and retry
            # this page; only give up if the second look is still logged out.
            if await self._relogin_shared_client(client):
                try:
                    resp = await self._get(client, url)
                except httpx.HTTPError as e:
                    return self._error_result(info, str(e))
                soup = make_soup(resp.text)
            if not _is_logged_in(soup):
                logger.error("Session expired while scraping %s", url)
                return self._error_result(info, "session expired — not logged in")

        title = _extract_title(soup) or info.get("title", slug)
        if title and title.lower().strip() in _UTILITY_PAGES:
            return self._error_result(info, "utility page")

        # Detect subscription/watchlist status from the main series page
        subscribed, watchlist = _detect_subscription_status(soup)

        # Extract alt titles
        alt_titles_from_page = _extract_alt_titles(soup)
        # Merge with alt_titles from index page if present
        alt_titles_from_info = info.get("alt_titles", [])
        alt_titles = list(dict.fromkeys(alt_titles_from_info + alt_titles_from_page))

        scrape_url = info.get("scrape_url", info["url"])
        if scrape_url.startswith("http://") or scrape_url.startswith("https://"):
            parsed_scrape = urlparse(scrape_url)
            season_base_url = f"{parsed_scrape.scheme}://{parsed_scrape.netloc}"
        else:
            season_base_url = self.site_url

        season_links = _extract_season_links(soup, slug, season_base_url)
        if not season_links:
            return self._error_result(info, "no seasons found")

        seasons_data = []
        total_watched, total_eps = 0, 0
        ignored_seasons = self._get_ignored_seasons()
        has_episode_zero = False
        stale_ignored = []

        season_pages = await self._fetch_season_pages(client, season_links)

        for (label, season_url), page in zip(season_links, season_pages, strict=True):
            if isinstance(page, BaseException):
                return self._error_result(info, f"season {label} fetch failed: {page}")
            with self._profiler.phase("parse"):
                episodes = parse_season_html(page)
            if episodes is None:
                # None means the page had no episode table at all, or a row
                # whose episode number could not be determined -- a scrape
                # failure. An empty list is different: the table is there
                # and genuinely empty, and that gets stored as 0 like before.
                return self._error_result(info, f"season {label}: episode table missing or unparseable")

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
                stale_ignored.append(
                    {
                        "slug": slug,
                        "season": label,
                        "url": f"{SITE_URL}/anime/stream/{slug}",
                        "title": title,
                    }
                )

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
            "url": scrape_url,
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
        if has_episode_zero:
            result["_has_episode_zero"] = True
        if stale_ignored:
            result["_stale_ignored_seasons"] = stale_ignored
        result["scrape_duration_seconds"] = time.perf_counter() - t_start
        return result

    @staticmethod
    def _error_result(info: dict, reason: str) -> dict:
        return {
            "title": info.get("title", ""),
            "link": info.get("link", ""),
            "url": info.get("url", ""),
            "total_seasons": 0,
            "total_episodes": 0,
            "watched_episodes": 0,
            "unwatched_episodes": 0,
            "subscribed": None,
            "watchlist": None,
            "seasons": [],
            "_error": True,
            "_error_reason": reason,
        }

    # ── Worker ──────────────────────────────────────────────────────────────

    async def _relogin_shared_client(self, client) -> bool:
        """Log the shared session back in after an expiry, at most once at a time.

        Every worker shares one client, so an expiry mid-run would otherwise
        fail every remaining series. Workers that hit it together must not
        each fire their own login -- that is what made a sibling site start
        refusing logins outright -- so the lock plus the counter means the
        first one re-logs in and the rest simply reuse the result.
        """
        async with self._client_lock:
            attempt = self._relogin_count
        if attempt >= _MAX_RELOGINS:
            return False
        async with self._client_lock:
            if self._relogin_count != attempt:
                return True  # someone else just refreshed it
            self._relogin_count += 1
            try:
                await self._login_client(client)
                logger.warning("Session had expired; logged back in (attempt %d)", self._relogin_count)
                return True
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logger.error("Re-login after session expiry failed: %s", exc)
                return False

    async def _acquire_client(self):
        """Hand out the single logged-in session that every worker shares.

        Each worker used to build its own session, so an N-worker run meant N
        logins of three requests each, every run. That is pure waste -- and it
        is not harmless: cycling worker counts during a benchmark made
        serienstream.to start refusing logins outright, because the logins,
        not the scraping, were what looked abusive.

        httpx.AsyncClient is built to be driven from many tasks at once and
        owns its connection pool, so one client sized for the whole run serves
        everybody: one login, and keep-alive connections that are reused
        across workers and across series instead of per worker.

        Reference-counted rather than opened and closed by the orchestrators,
        because several entry points spawn workers and each would otherwise
        need the same bookkeeping.
        """
        async with self._client_lock:
            if self._shared_client is None:
                self._shared_client = await self._create_logged_in_client()
            self._client_users += 1
            return self._shared_client

    async def _release_client(self) -> None:
        """Drop this worker's claim; close the session once the last one exits."""
        async with self._client_lock:
            self._client_users -= 1
            if self._client_users <= 0 and self._shared_client is not None:
                await self._shared_client.aclose()
                self._shared_client = None
                self._client_users = 0

    async def _worker(
        self,
        worker_id: int,
        queue: asyncio.Queue,
        results: list,
        progress: dict,
        total: int,
        predicted_rate: float | None = None,
    ):
        try:
            client = await self._acquire_client()
        except RuntimeError:
            logger.warning("Worker %d login failed, retrying...", worker_id)
            await asyncio.sleep(1)
            try:
                client = await self._acquire_client()
            except RuntimeError:
                logger.error("Worker %d login failed permanently", worker_id)
                return

        try:
            while True:
                self._check_interrupt_flag()
                if self._check_pause():
                    raise ScrapingPausedError("Pause file detected")

                try:
                    info = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                try:
                    result = await self._scrape_one_series(client, info)
                except (ScrapingPausedError, asyncio.CancelledError):
                    raise
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    # Contain a per-series failure the way the bs.to project
                    # already does: one unparseable page should cost that
                    # series, not abandon the rest of the queue.
                    logger.error(
                        "Worker %d unexpected error on %s: %s",
                        worker_id,
                        info.get("url", "?"),
                        exc,
                    )
                    result = self._error_result(info, f"unexpected error: {exc}")

                if result.get("_error"):
                    reason = result.get("_error_reason") or "scrape_error"
                    self.failed_links.append(
                        {
                            "url": info["url"],
                            "title": info.get("title", ""),
                            "link": info.get("link", ""),
                            "reason": reason,
                        }
                    )
                elif result.get("total_episodes", 0) == 0:
                    results.append(result)
                    self.failed_links.append(
                        {
                            "url": info["url"],
                            "title": result.get("title", info.get("title", "")),
                            "link": info.get("link", ""),
                            "reason": "empty_placeholder",
                        }
                    )
                else:
                    results.append(result)
                    if result.get("_has_episode_zero"):
                        self.failed_links.append(
                            {
                                "url": info["url"],
                                "title": result.get("title", info.get("title", "")),
                                "link": info.get("link", ""),
                                "reason": "episode_0_placeholder",
                            }
                        )
                    if result.get("_stale_ignored_seasons"):
                        with self._lock:
                            for entry in result["_stale_ignored_seasons"]:
                                self._stale_ignored_warnings.append(
                                    {
                                        "title": result.get("title", ""),
                                        "slug": entry["slug"],
                                        "season": entry["season"],
                                    }
                                )

                # Keep completed_links, progress, and the checkpoint snapshot
                # consistent under the lock. This prevents a crash window where
                # completed_links is ahead of the saved series_data.
                with self._lock:
                    link = info.get("link", "")
                    if link:
                        self.completed_links.add(link)
                    if info.get("url"):
                        self.attempted_urls.add(info["url"])
                    progress["done"] += 1
                    done = progress["done"]
                    if done % CHECKPOINT_EVERY == 0:
                        self.series_data = list(results)

                # Progress bar + ETA using per-series historical timings
                elapsed = time.perf_counter() - progress["start"]
                current_rate = done / elapsed if elapsed > 0 else 0

                if predicted_rate is not None and predicted_rate > 0:
                    # Blend: historical predicted rate dominates (95%+ initially),
                    # current session rate gains weight very slowly as it proves itself.
                    # Series are always the same, so last run's avg_scrape_seconds
                    # is the best predictor for next scrape time.
                    current_weight = min(0.05, 0.01 + (done / total) * 0.04)
                    rate = current_rate * current_weight + predicted_rate * (1 - current_weight)
                else:
                    rate = current_rate

                eta = (total - done) / rate if rate > 0 else 0
                eta_mins = f"{eta / 60:.1f}"
                pct = int((done / total) * 100)
                bar_len = 30
                filled = int(bar_len * done / total)
                bar = "█" * filled + "░" * (bar_len - filled)

                season_labels = [s.get("season", "?") for s in result.get("seasons", [])]
                season_info = f" [{','.join(season_labels)}]" if season_labels else ""

                # Subscription status indicators
                sub_parts = []
                if result.get("subscribed") is not None:
                    sub_parts.append(f"Sub:{'✓' if result['subscribed'] else '✗'}")
                if result.get("watchlist") is not None:
                    sub_parts.append(f"WL:{'✓' if result['watchlist'] else '✗'}")
                sub_info = f" ({' '.join(sub_parts)})" if sub_parts else ""

                ep0_warn = " ⚠ episode 0 detected" if result.get("_has_episode_zero") else ""

                if result.get("_error"):
                    reason = result.get("_error_reason", "Failed")
                    self._progress.write(
                        f"[{done}/{total}] [{bar}] {pct}% | ETA: {eta_mins}m"
                        f" | ⚠ {info.get('title', '?')}: {reason}"
                    )
                elif result["total_episodes"] == 0:
                    self._progress.write(
                        f"[{done}/{total}] [{bar}] {pct}% | ETA: {eta_mins}m"
                        f" | ⚠ {result['title']}{season_info}: No episodes{sub_info}"
                    )
                else:
                    self._progress.write(
                        f"[{done}/{total}] [{bar}] {pct}% | ETA: {eta_mins}m"
                        f" | ✓ {result['title']}{season_info}:"
                        f" {result['watched_episodes']}/{result['total_episodes']}"
                        f" watched{sub_info}{ep0_warn}"
                    )

                if done % CHECKPOINT_EVERY == 0:
                    await self.asave_checkpoint(include_data=False)
        finally:
            self._progress.flush()
            await self._release_client()

    # ── Async scrape orchestrators ──────────────────────────────────────────

    def _filter_completed(self, series_list: list[dict]) -> list[dict] | None:
        if not self.completed_links:
            return series_list
        before = len(series_list)
        filtered = [s for s in series_list if s.get("link") not in self.completed_links]
        if before != len(filtered):
            print(f"  Skipping {before - len(filtered)} already-completed anime")
        if not filtered:
            print("✓ All anime already scraped (from checkpoint)")
            return None
        return filtered

    def _compute_predicted_eta(self, series_list: list[dict]) -> float | None:
        """Sum per-series avg_scrape_seconds from the index for ETA prediction.

        Reads the existing series index and looks up each target series by slug.
        Returns predicted total seconds, or None if insufficient data.
        """
        try:
            if not os.path.exists(SERIES_INDEX_FILE):
                return None
            with open(SERIES_INDEX_FILE, encoding="utf-8") as f:
                data = json.load(f)
            items = data if isinstance(data, list) else list(data.values())
            # Build slug → avg_scrape_seconds map
            slug_map: dict[str, float] = {}
            for item in items:
                url = item.get("url", "") or item.get("link", "")
                slug = self.get_series_slug_from_url(url)
                avg = item.get("avg_scrape_seconds")
                if slug != "unknown" and isinstance(avg, (int, float)) and avg > 0:
                    slug_map[slug] = float(avg)

            total = 0.0
            matched = 0
            for s in series_list:
                slug = self.get_series_slug_from_url(s.get("link", ""))
                if slug in slug_map:
                    total += slug_map[slug]
                    matched += 1

            if matched >= max(3, len(series_list) * 0.1):
                return total
            return None
        except Exception:
            return None

    def _get_average_scrape_seconds(self) -> float | None:
        """Compute the average avg_scrape_seconds across all indexed series.

        Used as a fallback estimate when per-series slug matching fails
        (e.g. first scrape with no historical data for specific series).
        """
        try:
            if not os.path.exists(SERIES_INDEX_FILE):
                return None
            with open(SERIES_INDEX_FILE, encoding="utf-8") as f:
                data = json.load(f)
            items = data if isinstance(data, list) else list(data.values())
            values = [
                float(item["avg_scrape_seconds"])
                for item in items
                if isinstance(item.get("avg_scrape_seconds"), (int, float)) and item["avg_scrape_seconds"] > 0
            ]
            if len(values) >= 3:
                return sum(values) / len(values)
            return None
        except Exception:
            return None

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

        # Pre-compute predicted ETA from per-series historical timings
        predicted_seconds = self._compute_predicted_eta(filtered)
        if predicted_seconds:
            # Divide by workers: _compute_predicted_eta returns sequential total,
            # but we scrape in parallel so wall-clock time is roughly total / workers.
            predicted_seconds = predicted_seconds / n
            predicted_rate = len(filtered) / predicted_seconds
            print(f"→ Scraping {len(filtered)} anime with {n} session(s) (predicted ~{predicted_seconds / 60:.1f}m)...")
        else:
            # Fallback: use average of existing per-series timings from the index
            avg_seconds = self._get_average_scrape_seconds()
            estimated_seconds_per_series = avg_seconds or 1.0
            predicted_seconds = len(filtered) * estimated_seconds_per_series / n
            predicted_rate = len(filtered) / predicted_seconds
            print(f"→ Scraping {len(filtered)} anime with {n} session(s) (estimated ~{predicted_seconds / 60:.1f}m)...")

        tasks = [
            asyncio.create_task(
                self._worker(
                    i,
                    queue,
                    results,
                    progress,
                    len(filtered),
                    predicted_rate=predicted_rate,
                )
            )
            for i in range(n)
        ]
        try:
            await asyncio.gather(*tasks)
        except ScrapingPausedError:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.series_data = results
            raise
        except BaseException:
            # Any other failure (a parser bug, Ctrl+C, a cancelled task) used
            # to skip the assignment below, so every series scraped since the
            # last checkpoint was thrown away and the siblings were left
            # running detached. Keep the work and stop the pool, exactly as
            # the pause path does, then let the error travel on.
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.series_data = results
            raise

        self.series_data = results

    def _ignored_seasons_continue(self) -> bool:
        """After scraping ignored-season anime, check for changes and prompt.

        Returns True to continue scraping, False to stop.
        """
        has_stale = bool(self._stale_ignored_warnings)
        has_new_ep0 = any(f.get("reason") == "episode_0_placeholder" for f in self.failed_links)

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
        if answer != "y":
            print("✗ Scraping stopped. Saving progress...")
            self.save_checkpoint(include_data=True)
            if self.failed_links:
                self.save_failed_series()
            return False
        return True

    async def _async_run(
        self,
        single_url=None,
        url_list=None,
        new_only=False,
        retry_failed=False,
        account_source=None,
    ):
        """Async core of run()."""
        # Use a temp client for discovery, then close it
        tmp = await self._create_logged_in_client()
        try:
            print("✓ Logged in to aniworld.to")
            await self._async_run_inner(
                tmp,
                single_url=single_url,
                url_list=url_list,
                new_only=new_only,
                retry_failed=retry_failed,
                account_source=account_source,
            )
        finally:
            if not tmp.is_closed:
                await tmp.aclose()

    async def _async_run_inner(
        self,
        tmp,
        single_url=None,
        url_list=None,
        new_only=False,
        retry_failed=False,
        account_source=None,
    ):

        if single_url:
            self._checkpoint_mode = "single"
            main_url = self.normalize_to_series_url(single_url)
            m = _ANIME_PATH_RE.search(main_url)
            link = m.group(1) if m else main_url
            info = {"title": main_url.split("/")[-1], "link": link, "url": main_url}
            print(f"→ Scraping single anime: {main_url}")
            self.attempted_urls.add(main_url)
            result = await self._scrape_one_series(tmp, info)
            await tmp.aclose()
            if result.get("_error"):
                self.failed_links.append(info)
                self.series_data = []
            else:
                self.series_data = [result]
            # Single-anime runs have no partial resume state to preserve.
            self.clear_checkpoint()
            return

        if url_list:
            self._checkpoint_mode = self._checkpoint_mode or "batch"
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
            self._checkpoint_mode = "retry"
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
            await self._revalidate_ignored_series(tmp)
            print(f"→ Fetching {account_source} anime from account pages...")
            account_series = await self._get_account_series(tmp, source=account_source)
            self.all_discovered_series = account_series
            await tmp.aclose()
            if not account_series:
                print("✓ No anime found on account pages")
                return

            # New anime detection
            existing_slugs = self.load_existing_slugs()
            new_titles = [
                s["title"]
                for s in account_series
                if self.get_series_slug_from_url(s.get("link", "")) not in existing_slugs
            ]
            if new_titles:
                new_titles, _ = self._filter_new_entries([{"title": t, "url": ""} for t in new_titles], account_series)
                new_titles = [s["title"] for s in new_titles]
            if new_titles:
                print(f"\nℹ {len(new_titles)} new anime detected:")
                for t in new_titles[:10]:
                    print(f"  + {t}")
                if len(new_titles) > 10:
                    print(f"  ... and {len(new_titles) - 10} more")
                print()

            # Two-phase scraping: ignored-season anime first
            ignored_slugs_set = {slug for slug, _ in self._get_ignored_seasons()}
            ignored_batch = [
                s for s in account_series if self.get_series_slug_from_url(s.get("link", "")) in ignored_slugs_set
            ]
            rest_batch = [
                s for s in account_series if self.get_series_slug_from_url(s.get("link", "")) not in ignored_slugs_set
            ]

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
            self._checkpoint_mode = "new_only"
            await self._revalidate_ignored_series(tmp)
            print("→ Fetching anime list...")
            all_series = await self._get_all_series(tmp)
            await tmp.aclose()
            if not self._confirm_catalogue_size(all_series):
                return
            self.all_discovered_series = all_series
            self._check_ignored_vs_catalog(all_series)
            self._check_index_vs_catalog(all_series, quiet=True)
            existing_slugs = self.load_existing_slugs()
            ignored_slugs = self.get_ignored_slugs()
            new_list = [
                s
                for s in all_series
                if self.get_series_slug_from_url(s.get("link", "")) not in existing_slugs
                and self.get_series_slug_from_url(s.get("link", "")) not in ignored_slugs
            ]
            new_list, rename_titles = self._filter_new_entries(new_list, all_series)
            if rename_titles:
                print(f"\nℹ {len(rename_titles)} possible rename(s) of vanished anime (still scraped):")
                for t in sorted(rename_titles)[:10]:
                    print(f"  ~ {t}")
                if len(rename_titles) > 10:
                    print(f"  ... and {len(rename_titles) - 10} more")
                print()
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
        self._checkpoint_mode = "all_series"
        await self._revalidate_ignored_series(tmp)
        print("→ Fetching anime list...")
        all_series = await self._get_all_series(tmp)
        await tmp.aclose()
        if not self._confirm_catalogue_size(all_series):
            return
        self.all_discovered_series = all_series
        ignored_slugs = self.get_ignored_slugs()
        print(f"✓ Found {len(all_series)} anime")
        self._check_ignored_vs_catalog(all_series)
        self._check_index_vs_catalog(all_series, quiet=True)

        # New anime detection
        existing_slugs = self.load_existing_slugs()
        new_titles = [
            s["title"]
            for s in all_series
            if self.get_series_slug_from_url(s.get("link", "")) not in existing_slugs
            and self.get_series_slug_from_url(s.get("link", "")) not in ignored_slugs
        ]
        new_titles, rename_titles = self._filter_new_entries([{"title": t, "url": ""} for t in new_titles], all_series)
        new_titles = [s["title"] for s in new_titles]
        if new_titles or rename_titles:
            print(
                f"\nℹ {len(new_titles)} new anime detected "
                f"({len(rename_titles)} possible rename(s) of vanished anime, still scraped):"
            )
            for t in new_titles[:10]:
                print(f"  + {t}")
            if len(new_titles) > 10:
                print(f"  ... and {len(new_titles) - 10} more")
            if rename_titles:
                print("  Possible renames of vanished anime (still scraped):")
                for t in sorted(rename_titles)[:10]:
                    print(f"    ~ {t}")
                if len(rename_titles) > 10:
                    print(f"    ... and {len(rename_titles) - 10} more")
            print()

        if ignored_slugs:
            all_series = [
                s for s in all_series if self.get_series_slug_from_url(s.get("link", "")) not in ignored_slugs
            ]
            skipped = len(self.all_discovered_series) - len(all_series)
            if skipped:
                print(f"  Skipping {skipped} ignored anime")

        # Two-phase scraping: ignored-season anime first
        ignored_slugs_set = {slug for slug, _ in self._get_ignored_seasons()}
        ignored_batch = [s for s in all_series if self.get_series_slug_from_url(s.get("link", "")) in ignored_slugs_set]
        rest_batch = [
            s for s in all_series if self.get_series_slug_from_url(s.get("link", "")) not in ignored_slugs_set
        ]

        if ignored_batch:
            print(f"→ Phase 1: Scraping {len(ignored_batch)} anime with ignored seasons...")
            await self._scrape_list(ignored_batch, num_workers=1)
            if not self._ignored_seasons_continue():
                return

        n = NUM_WORKERS if self._use_parallel else 1
        await self._scrape_list(rest_batch, num_workers=n)
        print(f"\n✓ Successfully scraped {len(self.series_data)} anime")

    # ── Public API ───────────────────────────────────────────────────────────

    def run(
        self,
        single_url=None,
        url_list=None,
        new_only=False,
        resume_only=False,
        retry_failed=False,
        parallel=None,
        account_source=None,
        checkpoint_mode=None,
    ):
        """Main entry point: login, scrape, save checkpoint."""
        if parallel is not None:
            self._use_parallel = parallel
            print(f"→ Using {'multi-session' if parallel else 'single-session'} mode")
        else:
            self._use_parallel = True

        if checkpoint_mode is not None:
            self._checkpoint_mode = checkpoint_mode

        # Clear any stale pause file from a previous run
        self._clear_pause_file()

        # Register graceful Ctrl+C pause: signal handler creates the
        # pause file so workers finish their current series and then raise
        # ScrapingPausedError at the next checkpoint.
        def _signal_handler(signum, _frame):
            logger.info("Received signal %d — graceful pause requested", signum)
            print("\n⚠ Pause requested (Ctrl+C). Finishing current series...")
            self._create_pause_file()

        try:
            signal.signal(signal.SIGINT, _signal_handler)
            if hasattr(signal, "SIGTERM"):
                signal.signal(signal.SIGTERM, _signal_handler)
        except ValueError:
            # Not the main thread or signal not supported; ignore.
            pass

        try:
            if resume_only:
                if self.load_checkpoint():
                    print(f"→ Resuming from checkpoint ({len(self.completed_links)} anime already done)")
                else:
                    print("⚠ No checkpoint found. Starting fresh...")

            asyncio.run(
                self._async_run(
                    single_url=single_url,
                    url_list=url_list,
                    new_only=new_only,
                    retry_failed=retry_failed,
                    account_source=account_source,
                )
            )

            # Alert for empty anime (0 episodes)
            empty = [s for s in self.series_data if s.get("total_episodes", 0) == 0]
            if empty:
                print(f"\n⚠ {len(empty)} anime with 0 episodes:")
                for s in empty:
                    print(f"  • {s['title']} → {s['url']}")

            self.save_checkpoint(include_data=True)
            self.reconcile_failed_series()

        except ScrapingPausedError:
            self.paused = True
            self._clear_pause_file()
            self.save_checkpoint(include_data=True)
            if self.failed_links:
                self.save_failed_series()
        except (KeyboardInterrupt, SystemExit):
            self.save_checkpoint(include_data=True)
            if self.failed_links:
                self.save_failed_series()
            raise
        except Exception:  # pylint: disable=broad-exception-caught
            # An unexpected failure is exactly when the partial work matters
            # most: without this the run's scraped series and failed list were
            # both discarded and the next run started over from the last
            # checkpoint interval. Persist, then re-raise unchanged.
            logger.exception("Unexpected error during scrape — saving partial progress")
            self.save_checkpoint(include_data=True)
            if self.failed_links:
                self.save_failed_series()
            raise

    # ── Domain checking / probing ───────────────────────────────────────────

    async def _probe_one_site(self, site_url: str) -> dict:
        """Return probe result for a single site URL."""
        try:
            async with httpx.AsyncClient(timeout=HTTP_REQUEST_TIMEOUT, follow_redirects=True) as client:
                resp = await client.get(site_url)
            # Check if page loads without major errors (200-499)
            ok = resp.status_code < 500
            return {
                "site_url": site_url,
                "ok": ok,
                "status_code": resp.status_code,
                "reason": "reachable" if ok else "unexpected response",
            }
        except Exception as exc:
            return {
                "site_url": site_url,
                "ok": False,
                "status_code": None,
                "reason": str(exc),
            }

    async def probe_sites(self, site_urls: list[str] | None = None) -> list[dict]:
        """Probe multiple site URLs in parallel and report availability."""
        urls = site_urls or SITE_URLS
        return list(await asyncio.gather(*[self._probe_one_site(u) for u in urls]))

    async def get_catalogue_info_for_site(
        self,
        site_url: str,
    ) -> tuple[int | None, set[str]]:
        """Login once, fetch the catalogue once, and return (count, slugs).

        This avoids the duplicate login + full-page download that happened
        when count and slugs were fetched in separate calls.
        """
        previous_site_url = self.site_url
        try:
            self.site_url = site_url
            client = await self._create_logged_in_client()
            try:
                series = await self._get_all_series(client)
            finally:
                await client.aclose()
            slugs = set()
            for s in series:
                slug = self.get_series_slug_from_url(s.get("link", ""))
                if slug and slug != "unknown":
                    slugs.add(slug)
            return len(series), slugs
        except Exception as exc:
            logger.error("Error fetching catalogue info from %s: %s", site_url, exc)
            return None, set()
        finally:
            self.site_url = previous_site_url

    # Kept for backwards compatibility; prefer get_catalogue_info_for_site.
    # Kept for backwards compatibility; prefer get_catalogue_info_for_site.
    async def verify_series_url(
        self,
        client: httpx.AsyncClient,
        url: str,
    ) -> dict:
        """Fetch a series URL and return current title/availability info.

        Used to verify vanished/rename candidates without doing a full scrape.
        Returns a dict with keys: url, reachable, title, season_count, error.
        """
        result = {
            "url": url,
            "reachable": False,
            "title": None,
            "season_count": 0,
            "error": None,
        }
        try:
            resp = await client.get(url, follow_redirects=True)
            soup = make_soup(resp.text)
            error_code = _check_error_page(soup)
            if error_code:
                result["error"] = f"error_page_{error_code}"
                return result
            title = _extract_title(soup)
            season_count = _count_seasons_from_html(soup)
            result["reachable"] = True
            result["title"] = title
            result["season_count"] = season_count
        except httpx.HTTPError as exc:
            result["error"] = f"http_error_{exc}"
        except Exception as exc:  # pylint: disable=broad-exception-caught
            result["error"] = f"exception_{exc}"
        return result

    async def verify_vanished_and_candidates(
        self,
        vanished_entries: list[tuple[str, str]],
        candidate_entries: list[dict],
    ) -> tuple[list[tuple[str, str]], list[dict]]:
        """Re-fetch vanished URLs and rename candidates to verify accuracy.

        Args:
            vanished_entries: list of (title, url) tuples for vanished series.
            candidate_entries: list of new-entry dicts that might be renames.

        Returns:
            Tuple of (verified_vanished, verified_candidates) with updated
            titles and reachability flags stored in dict metadata.
        """
        client = httpx.AsyncClient(
            http2=True,
            headers={"User-Agent": UA},
            timeout=httpx.Timeout(REQUEST_TIMEOUT, connect=10.0),
            follow_redirects=True,
            limits=httpx.Limits(
                # One worker now has up to SEASON_CONCURRENCY season fetches
                # in flight at once, so a 2-connection pool would serialise
                # the fan-out straight back into the queue it was meant to
                # remove. Keepalive matches it so those connections survive
                # between series instead of re-handshaking each time.
                max_connections=self.pool_workers * SEASON_CONCURRENCY + 4,
                max_keepalive_connections=self.pool_workers * SEASON_CONCURRENCY + 4,
            ),
        )
        try:
            await self._login_client(client)
            all_urls = [url for _, url in vanished_entries if url]
            all_urls.extend(e.get("url", e.get("link", "")) for e in candidate_entries if e.get("url") or e.get("link"))
            unique_urls = sorted(set(all_urls))
            if not unique_urls:
                return vanished_entries, candidate_entries

            print(f"\n→ Verifying {len(unique_urls)} vanished/rename URL(s) with fresh scrape...")
            results = await asyncio.gather(
                *[self.verify_series_url(client, url) for url in unique_urls],
                return_exceptions=True,
            )
            info_by_url: dict[str, dict] = {}
            for res in results:
                if isinstance(res, Exception):
                    continue
                if isinstance(res, dict):
                    info_by_url[res["url"]] = res

            verified_vanished = []
            for title, url in vanished_entries:
                info = info_by_url.get(url, {})
                if info.get("reachable"):
                    verified_vanished.append((info.get("title") or title, url))
                else:
                    verified_vanished.append((title, url))

            verified_candidates = []
            for entry in candidate_entries:
                url = entry.get("url", entry.get("link", ""))
                info = info_by_url.get(url, {})
                new_entry = dict(entry)
                if info.get("reachable") and info.get("title"):
                    new_entry["title"] = info["title"]
                    new_entry["verified_season_count"] = info.get("season_count", 0)
                new_entry["_verified_reachable"] = info.get("reachable", False)
                new_entry["_verified_error"] = info.get("error")
                verified_candidates.append(new_entry)

            return verified_vanished, verified_candidates
        finally:
            if client and not client.is_closed:
                await client.aclose()
