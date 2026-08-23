"""Capture real pages from the live site as golden parser fixtures.

The whole point of the optimisation work is that parsing stays byte-for-byte
identical while it gets faster. That is only checkable against real markup,
so this pulls a spread of actual pages (series pages, season pages, a known
404) and records what today's parsers make of them. `test_golden_parse.py`
then fails the moment any change alters a single field.

Run from the project root:  python tests/capture_fixtures.py [--limit N]
Re-run it deliberately when the site's markup genuinely changes; review the
golden.json diff before committing it.
"""

import argparse
import asyncio
import gzip
import json
import os
import random
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.config import SERIES_INDEX_FILE, SITE_URL  # noqa: E402
from tests import fixture_spec  # noqa: E402

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures"
PAGE_DIR = FIXTURE_DIR / "pages"
GOLDEN_FILE = FIXTURE_DIR / "golden.json"

# The captured HTML is a logged-in view, so it carries the account name.
# Fixtures live in a public repo; the name does not.
#
# The name is read out of the page itself rather than guessed from a
# credential env var. Guessing was the original approach and it silently
# failed: two of the three projects store an email address, which is not the
# name these sites render, so every captured page kept the real account name
# while the scrub reported success.
_PROFILE_RE = re.compile(r"/user/profil/([^\"'/?#\s]+)", re.IGNORECASE)


def account_names_in(html: str) -> set[str]:
    """Every spelling of the logged-in account this page reveals."""
    names: set[str] = set()
    for raw in _PROFILE_RE.findall(html):
        name = raw.strip()
        if name and name.lower() != "testuser":
            names.add(name)
    for var in ("ANIWORLD_EMAIL", "BS_USERNAME", "STO_EMAIL", "BS_USER"):
        value = os.getenv(var, "").strip()
        if value:
            names.add(value)
            names.add(value.split("@")[0])
    return {n for n in names if len(n) > 2}


def scrub(html: str, extra_names: list[str] | None = None) -> str:
    """Replace every trace of the account with a placeholder."""
    names = account_names_in(html) | {n for n in (extra_names or []) if n}
    for name in sorted(names, key=len, reverse=True):
        html = re.sub(re.escape(name), "testuser", html, flags=re.IGNORECASE)
    return _PROFILE_RE.sub("/user/profil/testuser", html)


def pick_series(limit: int) -> list[str]:
    """Choose a spread of slugs: the biggest, the smallest, and a random tail."""
    with open(SERIES_INDEX_FILE, encoding="utf-8") as fh:
        data = json.load(fh)
    items = data if isinstance(data, list) else list(data.values())
    slugs = []
    for entry in items:
        m = re.search(fixture_spec.SLUG_RE, entry.get("url", "") or "")
        if m:
            slugs.append((entry.get("total_seasons", 0), entry.get("total_episodes", 0), m.group(1)))
    slugs.sort(reverse=True)
    picked = [s[2] for s in slugs[: limit // 3]]  # most seasons
    picked += [s[2] for s in slugs[-(limit // 3) :]]  # fewest
    rest = [s[2] for s in slugs if s[2] not in picked]
    random.Random(20260823).shuffle(rest)
    picked += rest[: limit - len(picked)]
    return list(dict.fromkeys(picked))


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=24, help="series pages to capture")
    args = ap.parse_args()

    PAGE_DIR.mkdir(parents=True, exist_ok=True)
    from src.scraper import AniWorldScraper  # noqa: PLC0415

    scraper = AniWorldScraper()
    client = await scraper._create_logged_in_client()
    account_names = [os.getenv(v, "") for v in ("ANIWORLD_EMAIL", "BS_USERNAME", "STO_EMAIL")]
    account_names = [n.split("@")[0] for n in account_names if n]

    golden: dict[str, dict] = {}
    try:
        targets: list[tuple[str, str, str]] = []
        for slug in pick_series(args.limit):
            targets.append((f"series__{slug}", SITE_URL + fixture_spec.SERIES_PATH.format(slug=slug), slug))
        # A page that must parse as an error, so the 404 path is pinned too.
        targets.append(
            (
                "error__missing",
                SITE_URL + fixture_spec.SERIES_PATH.format(slug="definitely-not-a-real-series-xyzzy"),
                "definitely-not-a-real-series-xyzzy",
            )
        )

        season_targets: list[tuple[str, str, str]] = []
        for name, url, slug in targets:
            try:
                resp = await client.get(url, follow_redirects=True)
            except Exception as exc:  # noqa: BLE001
                print(f"  skip {name}: {exc}")
                continue
            html = scrub(resp.text, account_names)
            (PAGE_DIR / f"{name}.html.gz").write_bytes(gzip.compress(html.encode("utf-8")))
            golden[name] = {"slug": slug, "result": fixture_spec.parse_all(html, slug, SITE_URL)}
            print(f"  {name}: {len(html) // 1024} KB")
            for label, season_url in golden[name]["result"].get("season_links") or []:
                season_targets.append((f"season__{slug}__{label}", season_url, slug))

        random.Random(20260823).shuffle(season_targets)
        for name, url, slug in season_targets[: args.limit * 2]:
            try:
                resp = await client.get(url, follow_redirects=True)
            except Exception as exc:  # noqa: BLE001
                print(f"  skip {name}: {exc}")
                continue
            html = scrub(resp.text, account_names)
            (PAGE_DIR / f"{name}.html.gz").write_bytes(gzip.compress(html.encode("utf-8")))
            golden[name] = {"slug": slug, "result": fixture_spec.parse_all(html, slug, SITE_URL)}
            print(f"  {name}: {len(html) // 1024} KB")
    finally:
        await client.aclose()

    GOLDEN_FILE.write_text(json.dumps(golden, indent=1, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    print(f"\nCaptured {len(golden)} pages -> {GOLDEN_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
