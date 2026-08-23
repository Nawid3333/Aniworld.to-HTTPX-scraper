"""
Configuration for the AniWorld.to HTTPX scraper.

Loads credentials from .env file, sets paths, and scraping options.
"""

import contextlib
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv


def configure_console() -> None:
    """Make arrow/box-drawing output safe on any code page.

    A redirected pipe or a legacy Windows code page falls back to cp1252,
    which cannot encode "→" or "─" -- printing the very first status
    line would kill the run with a UnicodeEncodeError. ``errors="replace"``
    guarantees no crash even where UTF-8 itself is refused.

    Called at import time because this module is the earliest one every
    entry point (main.py, the test suite) pulls in, and it prints on import.
    """
    for stream in (sys.stdout, sys.stderr):
        with contextlib.suppress(Exception):
            stream.reconfigure(encoding="utf-8", errors="replace")


configure_console()

# Load environment variables from .env file at import time so every module
# that imports from this config sees the correct values immediately.
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))


def _validate_and_normalize_url(url: str) -> str:
    """Validate and normalize a URL, raising ValueError for invalid URLs."""
    if not url:
        raise ValueError("URL cannot be empty")

    # Ensure URL has a scheme
    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    # Parse and validate
    try:
        parsed = urlparse(url)
        if not parsed.netloc:
            raise ValueError(f"Invalid URL: {url}")
        return url.rstrip("/")
    except Exception as e:
        raise ValueError(f"Invalid URL '{url}': {e}") from e


# Site configuration (primary + fallback mirrors)
# NOTE: The IP fallback only supports HTTP (no TLS). It is a last-resort
# fallback — credentials are sent unencrypted when this host is used.
_SITE_URLS = [
    "https://aniworld.to",
    "https://aniworld.cc",
    "http://186.2.175.111",
]

SITE_URLS = []
_seen = set()
for _url in _SITE_URLS:
    try:
        _normalized = _validate_and_normalize_url(_url)
        if _normalized not in _seen:
            _seen.add(_normalized)
            SITE_URLS.append(_normalized)
    except ValueError:
        print(f"⚠ Warning: Invalid site URL skipped: {_url}")

# Backwards-compatible alias: the first configured URL is the canonical primary.
SITE_URL = SITE_URLS[0] if SITE_URLS else ""

# Compute valid series hosts from SITE_URLS for URL validation
_VALID_HOSTS = set()
for _url in SITE_URLS:
    try:
        _parsed = urlparse(_url)
        if _parsed.netloc:
            _VALID_HOSTS.add(_parsed.netloc)
    except Exception:
        pass
VALID_SERIES_HOSTS = frozenset(_VALID_HOSTS)

# ==================== CREDENTIALS ====================
EMAIL = os.getenv("ANIWORLD_EMAIL", "")
PASSWORD = os.getenv("ANIWORLD_PASSWORD", "")

# ==================== DIRECTORIES ====================
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
LOGS_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")

Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
Path(LOGS_DIR).mkdir(parents=True, exist_ok=True)

# ==================== FILE PATHS ====================
SERIES_INDEX_FILE = os.path.join(DATA_DIR, "series_index.json")

# Default batch file for single/batch URL import
# Edit DEFAULT_BATCH_FILE_PATH below to change the default batch file
DEFAULT_BATCH_FILE_PATH = os.path.join(os.path.dirname(__file__), "..", "series_urls.txt")
DEFAULT_BATCH_FILE = os.path.abspath(DEFAULT_BATCH_FILE_PATH)

# ==================== SCRAPING SETTINGS ====================
# Measured, not guessed: a worker sweep over a representative sample of
# this catalogue (median 1 season, matching the real distribution) found
# 43.9 pages/s at 8, vs 35.9 at 6 and 31.3 at 12.
# Re-measured after workers began sharing one logged-in session: the old
# per-worker login both skewed the comparison and cost real throughput.
# Past the peak the season fan-out already keeps pool_workers *
# SEASON_CONCURRENCY requests in flight, so more workers only add load.
NUM_WORKERS = int(os.getenv("ANIWORLD_MAX_WORKERS", "8"))

# Season pages of one series are independent GETs. Fetching them one after
# another made a series' scrape time scale linearly with its season count,
# so they are fanned out this many at a time instead. Total requests in
# flight is NUM_WORKERS * SEASON_CONCURRENCY -- raise either with care, and
# only alongside the RateGuard that reacts to the site pushing back.
SEASON_CONCURRENCY = int(os.getenv("ANIWORLD_SEASON_CONCURRENCY", "4"))


# Checkpoint frequency: serialize resume state every N completed series.
# Smaller index (≈18 MB) → moderate interval.
CHECKPOINT_EVERY = int(os.getenv("ANIWORLD_CHECKPOINT_EVERY", "25"))

# ==================== TIMEOUTS ====================
HTTP_REQUEST_TIMEOUT = 20.0

# ==================== LOGGING ====================
LOG_FILE = os.path.join(LOGS_DIR, "aniworld_backup.log")

print(f"✓ Config loaded (DATA_DIR: {os.path.abspath(DATA_DIR)})")
