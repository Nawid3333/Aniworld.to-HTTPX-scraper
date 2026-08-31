"""Genre statistics for aniworld.to (menu option 7).

Self-contained by design: this module composes the existing scraper, index and
atomic-write helpers, and keeps its own data file. Nothing outside it -- other
than the menu wiring in main.py -- is touched, so the series index, the merge
paths and the golden parser fixtures stay exactly as they were.

Genres are read from *series pages*, never from the catalogue listing. The
listing files each series under one genre only; the series page carries all of
them. Every genre counts, all genres count equally, and a series counts in each
of its genres -- so the column totals are deliberately larger than the number of
series.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
import time
from datetime import datetime
from typing import Protocol

from config.config import DATA_DIR, NUM_WORKERS, SERIES_INDEX_FILE, SITE_URL
from src.atomic_io import atomic_write_json
from src.index_manager import IndexManager, get_episode_counts, paginate_list
from src.scraper import AniWorldScraper, ProgressWriter, _extract_title, _is_logged_in, make_soup

logger = logging.getLogger(__name__)

GENRE_INDEX_FILE = os.path.join(DATA_DIR, "genre_index.json")
GENRE_REPORT_FILE = os.path.join(DATA_DIR, "genre_report.json")

SCHEMA_VERSION = 1

# Partial progress is flushed this often, so an interrupted run resumes instead
# of starting over. The file is derived, so a half-written one is never a loss.
SAVE_EVERY = 250

# Shown in the menu so the cost of option 1 is visible before choosing it.
SCRAPE_ESTIMATE = "~1 min"

_WS_RE = re.compile(r"\s+")
_DIGITS_RE = re.compile(r"(\d+)")


# ── Genre identity ──────────────────────────────────────────────────────────


def normalize_genre_key(value: object) -> str:
    """Return the stable lookup key for a genre href or display string.

    The same genre reaches us spelled several ways -- "Fighting-Shounen" as link
    text, "fighting-shounen" in the href. Keyed as-is one genre becomes two
    categories and every ratio is wrong, so everything that groups, counts or
    diffs genres goes through this one function.
    """
    if not value or not isinstance(value, str):
        return ""
    text = value.strip()
    # Both spellings occur: these sites emit absolute ("/genre/x") and relative
    # ("genre/x") hrefs, and BS.to supplies no href at all -- only display text.
    if "/genre/" in text:
        text = text.split("/genre/", 1)[1]
    elif text.startswith("genre/"):
        text = text[len("genre/") :]
    text = text.strip("/").split("/")[0].split("?")[0].split("#")[0]
    text = text.strip().rstrip(",").strip().casefold()
    return _WS_RE.sub("-", text)


# ── Site-specific parser ────────────────────────────────────────────────────
# The only part of this module that differs between the three scrapers.


def _scan_genre_block(soup) -> tuple[list[tuple[str, str]], int, int, int | None]:
    """One traversal of div.genres li -> (genres, visible, raw_anchors, hidden).

    extract_genres() and the truncation check used to each run their own
    soup.select() over the same block -- three passes over the same handful
    of elements for every one of ~2,462 pages. Measured not to matter (a
    profiled live run put parse at 9.9% of wall time against 90.1% network,
    this project's usual story -- see PhaseProfiler in scraper.py), but doing
    it in one pass is free and is what "iterate once" means in practice.
    """
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    visible = 0
    raw_anchors = 0
    hidden: int | None = None
    past_button = False
    for element in soup.select("div.genres li"):
        button = element.find("button", class_="hiddenArea")
        if button is not None:
            past_button = True
            match = _DIGITS_RE.search(button.get_text())
            hidden = int(match.group(1)) if match else None
            continue
        anchor = element.find("a", attrs={"itemprop": "genre"})
        if anchor is None:
            continue
        raw_anchors += 1
        if not past_button:
            visible += 1
        label = anchor.get_text(strip=True)
        key = normalize_genre_key(str(anchor.get("href", ""))) or normalize_genre_key(label)
        if key and key not in seen:
            seen.add(key)
            out.append((key, label or key))
    return out, visible, raw_anchors, hidden


def extract_genres(soup) -> list[tuple[str, str]]:
    """Return [(key, label), ...] for every genre on a series page.

    Selecting anchors by ``itemprop`` (inside _scan_genre_block) reaches past
    the "+ N" button that sits *mid-list* inside the <ul>: aniworld hides the
    tail of a long genre list with CSS, but every genre is present in the
    HTML. Iterating <li> elements' text instead would return "+ 2" as a genre
    and drop the ones after it.
    """
    return _scan_genre_block(soup)[0]


def _hidden_genre_count(soup) -> int | None:
    """How many genres the page admits to hiding, or None if it says nothing.

    A free tripwire: the site tells us the number, so a markup change that
    starts costing us genres shows up as a warning instead of silently smaller
    numbers. Kept as its own entry point for callers that only want this.
    """
    return _scan_genre_block(soup)[3]


def _check_truncation(slug: str, visible: int, raw_anchors: int, hidden: int | None) -> None:
    """Warn when the page hid more genre anchors than we parsed past.

    Takes the counts _scan_genre_block already computed rather than
    re-scanning the page: the worker loop needs both the genres and this
    check from the same page, and a second soup.select() pass over the same
    handful of elements would undo the "one pass" property that function
    documents.

    Compares against raw *anchor* count, not the deduplicated genre count:
    some live pages genuinely repeat a genre (astra-lost-in-space lists
    "Action" twice), and that repeat is correctly dropped by extract_genres'
    own dedup -- it must not also trip this tripwire.
    """
    if hidden is None or raw_anchors == 0:
        return
    if raw_anchors != visible + hidden:
        logger.warning(
            "Genre truncation mismatch for %s: %d raw anchors, page says %d visible + %d hidden",
            slug,
            raw_anchors,
            visible,
            hidden,
        )


# ── Storage ─────────────────────────────────────────────────────────────────


def _empty() -> dict:
    return {
        "version": SCHEMA_VERSION,
        "generated": "",
        "host": "",
        "catalogue_total": 0,
        "scraped_count": 0,
        "labels": {},
        "titles": {},
        "series": {},
        "previous_series": {},
    }


def load_genres() -> dict:
    """Load the genre file, returning an empty skeleton for anything unusable.

    Every field here is derived from the site plus the index, so a corrupt or
    unrecognised file is never a loss -- it just means the next scrape rebuilds
    it. Nothing in here is worth raising over.
    """
    if not os.path.exists(GENRE_INDEX_FILE):
        return _empty()
    try:
        with open(GENRE_INDEX_FILE, encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Could not read %s (%s) -- starting fresh", GENRE_INDEX_FILE, exc)
        return _empty()
    if not isinstance(data, dict) or data.get("version") != SCHEMA_VERSION:
        logger.warning("Unrecognised genre file version -- starting fresh")
        return _empty()
    base = _empty()
    base.update({k: v for k, v in data.items() if k in base})
    if (
        not isinstance(base["series"], dict)
        or not isinstance(base["labels"], dict)
        or not isinstance(base["titles"], dict)
    ):
        return _empty()
    return base


def save_genres(data: dict) -> None:
    """Write the genre file atomically, with sorted keys for clean diffs.

    backup=False on purpose: this file is fully derived, so rotating three
    generations of a ~2-3 MB file would cost disk for nothing.
    """
    payload = dict(data)
    payload["series"] = {k: payload["series"][k] for k in sorted(payload["series"])}
    payload["labels"] = {k: payload["labels"][k] for k in sorted(payload["labels"])}
    payload["titles"] = {k: payload["titles"][k] for k in sorted(payload.get("titles") or {})}
    payload["previous_series"] = {
        k: payload["previous_series"][k] for k in sorted(payload.get("previous_series") or {})
    }
    atomic_write_json(GENRE_INDEX_FILE, payload, backup=False)


# ── Scraping ────────────────────────────────────────────────────────────────


def _targets(scraper, catalogue: list[dict]) -> list[tuple[str, str]]:
    """Return [(slug, url), ...] for the catalogue, minus ignored series.

    The URL is rebuilt from the *active* host plus the entry's path rather than
    taken from ``url``, which the catalogue hard-codes to the primary host.
    """
    ignored = scraper.get_ignored_slugs()
    seen: set[str] = set()
    out: list[tuple[str, str]] = []
    for entry in catalogue:
        path = entry.get("link") or ""
        slug = scraper.get_series_slug_from_url(path or entry.get("url", ""))
        if not slug or slug == "unknown" or slug in seen or slug in ignored:
            continue
        seen.add(slug)
        out.append((slug, f"{scraper.site_url}{path}" if path else entry.get("url", "")))
    return out


async def _scrape_async(site_url: str | None, data: dict, state: dict, *, refetch_all: bool = False) -> None:
    scraper = AniWorldScraper()
    scraper.site_url = site_url or SITE_URL
    # Private helpers on purpose: reusing the scraper's logged-in client and its
    # retry/rate-guard GET is the whole reason this feature needs no changes to
    # scraper.py. Re-implementing either would duplicate tuned behaviour.
    client = await scraper._create_logged_in_client()  # noqa: SLF001
    try:
        print("\n→ Fetching catalogue...")
        catalogue = await scraper._get_all_series(client)  # noqa: SLF001
        targets = _targets(scraper, catalogue)
        data["host"] = scraper.site_url
        data["catalogue_total"] = len(targets)
        results, labels, titles = data["series"], data["labels"], data["titles"]
        # A fresh pass re-fetches everything rather than filtering by what's
        # already in `results` -- `results` is *not* cleared for a refresh
        # (see scrape_genres), so filtering here would wrongly see the whole
        # catalogue as already done and fetch nothing.
        todo = list(targets) if refetch_all else [t for t in targets if t[0] not in results]
        if not refetch_all and results:
            print(f"  resuming — {len(results):,}/{len(targets):,} already known")
        print(f"✓ {len(targets):,} series in catalogue — fetching {len(todo):,} pages\n")
        if not todo:
            return

        queue: asyncio.Queue = asyncio.Queue()
        for item in todo:
            queue.put_nowait(item)
        lock = asyncio.Lock()
        progress = ProgressWriter()
        start = state["start"]
        total = len(todo)

        async def worker() -> None:
            while True:
                try:
                    slug, url = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                genres: list[tuple[str, str]] = []
                title = slug
                ok = True
                try:
                    resp = await scraper._get(client, url)  # noqa: SLF001
                    with scraper._profiler.phase("parse"):  # noqa: SLF001
                        soup = make_soup(resp.text)
                        genres, visible, raw_anchors, hidden = _scan_genre_block(soup)
                        _check_truncation(slug, visible, raw_anchors, hidden)
                        title = _extract_title(soup) or slug
                    if not genres:
                        state["empty"] += 1
                    if not state["logged_out"] and not _is_logged_in(soup):
                        state["logged_out"] = True
                        logger.warning("Session expired mid-run; genres still parse anonymously")
                except Exception as exc:  # noqa: BLE001 - one bad page must never end the run
                    ok = False
                    state["failed"] += 1
                    logger.debug("Genre fetch failed for %s: %s", slug, exc)
                snapshot = None
                async with lock:
                    if ok:
                        # A failed slug is left out of `results` entirely --
                        # recording it as "genres: []" would look identical to
                        # a page that was fetched fine and genuinely has none,
                        # and it would then never be retried on the next run
                        # (todo is everything not already in `results`).
                        results[slug] = [key for key, _ in genres]
                        titles[slug] = title
                        for key, label in genres:
                            labels.setdefault(key, label)
                    state["done"] += 1
                    done = state["done"]
                    if done % SAVE_EVERY == 0:
                        # dict.copy() is a cheap, synchronous O(n) op -- doing it
                        # here, still under the lock, gives the background write a
                        # frozen snapshot instead of a dict other workers keep
                        # mutating on the main thread while it serialises.
                        data["scraped_count"] = len(results)
                        snapshot = dict(data)
                        snapshot["series"] = dict(results)
                        snapshot["labels"] = dict(labels)
                        snapshot["titles"] = dict(titles)
                    elapsed = time.perf_counter() - start
                    rate = done / elapsed if elapsed > 0 else 0
                    eta = (total - done) / rate if rate > 0 else 0
                    progress.write(
                        f"  [{done:,}/{total:,}] {rate:.1f}/s  ETA {int(eta // 60)}m{int(eta % 60):02d}s  {slug}"
                    )
                if snapshot is not None:
                    # atomic_write_json is synchronous disk I/O (write + fsync +
                    # rename); run it off the event loop on the frozen snapshot so
                    # a periodic checkpoint never stalls every other worker's
                    # in-flight request for its duration. A write failure here
                    # (disk full, permissions) must not take the whole scrape
                    # down with it -- the next periodic save gets another try,
                    # and the final save in scrape_genres() is guarded too.
                    try:
                        await asyncio.to_thread(save_genres, snapshot)
                    except OSError as exc:
                        logger.warning("Periodic genre-index checkpoint failed: %s", exc)

        try:
            await asyncio.gather(*(worker() for _ in range(max(1, NUM_WORKERS))))
        finally:
            progress.flush()
        scraper._profiler.report(wall=time.perf_counter() - state["start"])  # noqa: SLF001
    finally:
        await client.aclose()


def scrape_genres(site_url: str | None = None) -> dict:
    """Fetch every series page and record its genres. Resumable, never fatal."""
    data = load_genres()
    was_complete = data["catalogue_total"] > 0 and data["scraped_count"] >= data["catalogue_total"]
    fresh_pass = was_complete or not data["series"]
    if fresh_pass:
        # A finished run means this is a refresh: snapshot it as the baseline
        # the change list diffs against. `series`/`titles` are deliberately
        # *not* cleared here -- _scrape_async() overwrites each slug in place
        # as its page completes, so Ctrl+C partway through a refresh leaves
        # whatever hasn't been re-fetched yet instead of wiping thousands of
        # series down to whatever fraction finished before the interrupt.
        # `labels` is cheap to rebuild from scratch and is cleared so a
        # renamed genre's display text can't get stuck on setdefault's
        # first-seen-wins.
        data["previous_series"] = dict(data["series"])
        data["labels"] = {}
    state = {"done": 0, "empty": 0, "failed": 0, "logged_out": False, "start": time.perf_counter()}
    interrupted = False
    try:
        asyncio.run(_scrape_async(site_url, data, state, refetch_all=fresh_pass))
    except KeyboardInterrupt:
        interrupted = True
        print("\n⚠ Interrupted — saving progress so far...")
    except Exception as exc:  # noqa: BLE001
        print(f"\n✗ Genre scrape failed: {exc}")
        logger.exception("Genre scrape failed")
    data["scraped_count"] = len(data["series"])
    data["generated"] = datetime.now().isoformat()
    try:
        save_genres(data)
    except OSError as exc:
        print(f"\n✗ Could not save genre data: {exc}")
        logger.exception("Failed to save %s", GENRE_INDEX_FILE)

    elapsed = time.perf_counter() - state["start"]
    print(f"\n✓ {data['scraped_count']:,}/{data['catalogue_total']:,} series recorded in {elapsed:.0f}s")
    if state["empty"]:
        print(f"  {state['empty']:,} series returned no genre block")
    if state["failed"]:
        print(f"  {state['failed']:,} pages failed to fetch")
    if state["logged_out"]:
        print("  ⚠ the session expired during the run (genres still parsed anonymously)")
    if interrupted:
        print("  Run option 1 again to continue where this stopped.")
    print(f"  Saved to {GENRE_INDEX_FILE}\n")
    return data


# ── Analysis ────────────────────────────────────────────────────────────────


class _HasSeriesIndex(Protocol):
    """The only part of IndexManager this module reads -- real or faked in tests."""

    series_index: dict


def _index_by_slug(index: _HasSeriesIndex) -> dict:
    """Map slug -> index entry using the scraper's own slug function.

    Trap worth naming: the three scrapers do not agree about slugs. BS.to has no
    ``_extract_slug_from_field`` at all, S.to's two slug helpers disagree about
    case, and BS.to slugs carry capitals. Both sides of this join therefore go
    through ``get_series_slug_from_url`` and nothing else -- a case mismatch here
    would zero every number with no error anywhere.
    """
    scraper = AniWorldScraper()
    out = {}
    for entry in index.series_index.values():
        slug = scraper.get_series_slug_from_url(entry.get("link") or entry.get("url") or "")
        if slug and slug != "unknown":
            out[slug] = entry
    return out


def build_snapshot(genre_data: dict, by_slug: dict) -> dict:
    """Count done/indexed per genre.

    A series counts in *every* genre it carries -- no deduplication, no main
    genre -- so the category totals add up to more than the series count. That
    is the intended reading: "how much of this genre have I finished".

    Only series present in the local index are counted. A "how many does the
    site have in this genre" column was tried and dropped: on real data the
    index and the catalogue are in near-perfect sync (verified: 0 of 34
    categories differed on a complete scrape), so it only ever repeated the
    Watched column's own denominator. Index-vs-catalogue staleness is already
    surfaced once, globally, by the main scrape's own catalogue-size check --
    it does not need a second, per-genre copy here.
    """
    categories: dict[str, dict] = {}
    without_genres: list[str] = []
    done_series = 0
    indexed_series = 0

    for slug, keys in genre_data["series"].items():
        entry = by_slug.get(slug)
        is_indexed = entry is not None
        is_done = False
        if is_indexed:
            total, watched = get_episode_counts(entry)
            is_done = total > 0 and watched == total
            indexed_series += 1
            done_series += int(is_done)
        if not keys:
            without_genres.append(slug)
            continue
        if not is_indexed:
            continue
        for key in keys:
            cat = categories.setdefault(key, {"done": 0, "indexed": 0})
            cat["indexed"] += 1
            cat["done"] += int(is_done)

    return {
        "categories": categories,
        "without_genres": sorted(without_genres),
        "indexed_without_genre_data": sorted(set(by_slug) - set(genre_data["series"])),
        "indexed_series": indexed_series,
        "done_series": done_series,
    }


def _is_partial(data: dict) -> bool:
    """A scrape that stopped before covering the whole catalogue.

    One definition, used everywhere a caller needs to know whether the
    numbers on screen are the whole picture -- so the three sites this
    module's design centers on can never drift apart on what "partial" means.
    """
    return 0 < data["scraped_count"] < data["catalogue_total"]


def _format_when(data: dict) -> str:
    """Render the last-scrape timestamp for display, or "unknown" if unset."""
    return data["generated"][:16].replace("T", " ") if data["generated"] else "unknown"


def _load_snapshot(data: dict) -> dict:
    """Join genre data against the index and count done/indexed per genre.

    The one non-trivial setup step show_stats() and export_report() both
    need -- building the index, resolving every entry to a slug, and
    counting -- lives here once instead of as two independent copies that a
    future fix to the join would have to be applied to twice.
    """
    index = IndexManager(SERIES_INDEX_FILE)
    by_slug = _index_by_slug(index)
    return build_snapshot(data, by_slug)


def diff_snapshots(old: dict, new: dict) -> dict:
    """Compare two slug -> genres maps and describe what moved."""
    added_to_category: list[tuple[str, list[str]]] = []
    changed: list[tuple[str, list[str], list[str]]] = []
    for slug, keys in new.items():
        if slug not in old:
            if keys:
                added_to_category.append((slug, sorted(keys)))
            continue
        before, after = set(old[slug]), set(keys)
        if before != after:
            changed.append((slug, sorted(after - before), sorted(before - after)))
    old_cats = {k for keys in old.values() for k in keys}
    new_cats = {k for keys in new.values() for k in keys}
    return {
        "new_series": sorted(added_to_category),
        "changed": sorted(changed),
        "new_categories": sorted(new_cats - old_cats),
        "gone_categories": sorted(old_cats - new_cats),
    }


# ── Rendering ───────────────────────────────────────────────────────────────


def _bar(percent: float, width: int = 10) -> str:
    filled = max(0, min(width, int(round(percent / 100.0 * width))))
    return "█" * filled + "░" * (width - filled)


def _table_lines(rows: list[tuple[str, int, int]]) -> list[str]:
    """Render (label, done, indexed) rows, fitting the terminal."""
    if not rows:
        return []
    term = max(shutil.get_terminal_size().columns, 60)
    watched_txt = [f"{d:,}/{i:,}" for _, d, i in rows]
    w_col = max([len(t) for t in watched_txt] + [len("Watched")])
    bar_w = 10
    overhead = 2 + 2 + w_col + 2 + bar_w + 1 + 4
    n_col = max(10, min(max(len(r[0]) for r in rows), term - overhead))

    head = f"  {'GENRES':<{n_col}}  {'Watched':>{w_col}}  {'Progress':<{bar_w + 5}}"
    rule = f"  {'─' * n_col}  {'─' * w_col}  {'─' * (bar_w + 5)}"
    lines = [head.rstrip(), rule]

    for (label, done, indexed), wt in zip(rows, watched_txt, strict=True):
        percent = (done / indexed * 100) if indexed else 0.0
        name = label if len(label) <= n_col else label[: n_col - 1] + "…"
        line = f"  {name:<{n_col}}  {wt:>{w_col}}  {_bar(percent, bar_w)} {percent:>3.0f}%"
        lines.append(line.rstrip())
    return lines


# ── Views ───────────────────────────────────────────────────────────────────


def _sorted_rows(categories: dict, labels: dict) -> list[tuple[str, int, int]]:
    """Most-completed first, so finished genres are visible at a glance and
    what needs the most attention settles at the bottom."""

    def rank(item):
        key, cat = item
        ratio = (cat["done"] / cat["indexed"]) if cat["indexed"] else 0.0
        return (-ratio, -cat["indexed"], labels.get(key, key).casefold())

    return [(labels.get(key, key), cat["done"], cat["indexed"]) for key, cat in sorted(categories.items(), key=rank)]


def _change_lines(data: dict, changes: dict) -> list[str]:
    """One line per change, using display labels and series titles rather than keys/slugs."""
    label = data["labels"]
    title = data["titles"]
    lines = []
    for slug, keys in changes["new_series"]:
        lines.append(f"    + {title.get(slug, slug)} is new in {', '.join(label.get(k, k) for k in keys)}")
    for slug, gained, lost in changes["changed"]:
        parts = []
        if gained:
            parts.append("+ " + ", ".join(label.get(k, k) for k in gained))
        if lost:
            parts.append("- " + ", ".join(label.get(k, k) for k in lost))
        lines.append(f"    ~ {title.get(slug, slug)}: {'  '.join(parts)}")
    for key in changes["new_categories"]:
        lines.append(f"    + new category: {label.get(key, key)}")
    for key in changes["gone_categories"]:
        lines.append(f"    - category gone: {label.get(key, key)}")
    return lines


def show_stats(site_url: str | None = None) -> None:
    """Print watched/total per genre, joined against the series index."""
    data = load_genres()
    if not data["series"]:
        print("\n→ No genre data yet. Run option 1 (Scrape genres) first.\n")
        return

    snap = _load_snapshot(data)
    categories = snap["categories"]
    if not categories:
        print("\n[WARN] Genre data exists but no genres were parsed - the parser may be broken.\n")
        return

    host = data["host"] or site_url or SITE_URL
    print(
        f"\n→ Watch Stats of Categories   ({host} · scraped {_format_when(data)} · {data['scraped_count']:,} series)\n"
    )

    if _is_partial(data):
        print(
            f"  ⚠ PARTIAL — {data['scraped_count']:,}/{data['catalogue_total']:,} series scraped; totals are incomplete"
        )
        print("    Run option 1 again to finish.\n")

    changes = diff_snapshots(data.get("previous_series") or {}, data["series"])
    lines = _change_lines(data, changes)
    if lines:
        print(f"  ⚠ {len(lines)} change(s) since you last checked")
        for line in lines[:25]:
            print(line)
        if len(lines) > 25:
            print(f"    ... and {len(lines) - 25} more")
        print()
        # Viewing the change list marks it as seen: the next call to show_stats
        # diffs against *this* moment, not against the last scrape forever.
        # Only a scrape introduces new changes to report.
        data["previous_series"] = dict(data["series"])
        save_genres(data)

    for line in _table_lines(_sorted_rows(categories, data["labels"])):
        print(line)

    print(
        f"\n  Totals: {snap['done_series']:,}/{snap['indexed_series']:,} series fully watched, "
        f"across {len(categories)} genres"
    )
    print("  (column totals exceed the series count — a series counts in each of its genres)")
    if snap["without_genres"]:
        print(f"  {len(snap['without_genres']):,} series returned no genre data")
    if snap["indexed_without_genre_data"]:
        print(f"  {len(snap['indexed_without_genre_data']):,} indexed series were not in the catalogue")
    print()


def export_report(site_url: str | None = None) -> None:
    """Write the full category breakdown to data/genre_report.json."""
    data = load_genres()
    if not data["series"]:
        print("\n→ No genre data yet. Run option 1 (Scrape genres) first.\n")
        return

    snap = _load_snapshot(data)
    rows = _sorted_rows(snap["categories"], data["labels"])

    titles = data["titles"]
    report = {
        "generated": datetime.now().isoformat(),
        "host": data["host"] or site_url or SITE_URL,
        "genre_data_scraped": data["generated"],
        "catalogue_total": data["catalogue_total"],
        "scraped_count": data["scraped_count"],
        "partial": _is_partial(data),
        "series_indexed": snap["indexed_series"],
        "series_fully_watched": snap["done_series"],
        "categories": [
            {
                "genre": label,
                "done": done,
                "indexed": indexed,
                "percent": round(done / indexed * 100, 1) if indexed else 0.0,
            }
            for label, done, indexed in rows
        ],
        "changes": diff_snapshots(data.get("previous_series") or {}, data["series"]),
        "series_without_genres": snap["without_genres"],
        "indexed_without_genre_data": snap["indexed_without_genre_data"],
        # Title and genres live together per series -- a separate top-level
        # "titles" map meant every consumer had to cross-reference two dicts
        # by slug just to answer "what is this series and what does it carry".
        "series": {
            slug: {"title": titles.get(slug, slug), "genres": genres} for slug, genres in data["series"].items()
        },
    }
    atomic_write_json(GENRE_REPORT_FILE, report, backup=False)
    print(f"\n[OK] Genre report written to {GENRE_REPORT_FILE}")
    print(f"  {len(rows)} genres · {snap['done_series']:,}/{snap['indexed_series']:,} series fully watched\n")
    if snap["indexed_without_genre_data"]:
        print("  Indexed series with no genre data:")
        paginate_list(snap["indexed_without_genre_data"], lambda slug: f"    - {titles.get(slug, slug)}")
        print()


# ── Menu ────────────────────────────────────────────────────────────────────


def _status_lines(data: dict) -> list[str]:
    """Describe the genre data on hand, so a stale file is visible up front."""
    if not data["series"]:
        return ["Genre data: none yet — run 1 first"]
    genres = {key for keys in data["series"].values() for key in keys}
    lines = [
        f"Genre data: {data['scraped_count']:,} series · {len(genres)} genres",
        f"            scraped {_format_when(data)}",
    ]
    if _is_partial(data):
        lines.append(f"            ⚠ PARTIAL — {data['scraped_count']:,}/{data['catalogue_total']:,}")
    return lines


def menu(site_url: str | None = None) -> None:
    """Option 7: the whole feature, self-contained."""
    while True:
        print("\n→ Watch Stats of Categories")
        # Loaded once per redraw -- scrape_genres() below can change it on
        # disk between passes, but it does not change while this pass prints.
        for line in _status_lines(load_genres()):
            print(f"  {line}")
        print(f"\n  1. Scrape genres      (refresh, {SCRAPE_ESTIMATE})")
        print("  2. Show stats (watched / total)")
        print("  3. Export genre report")
        print("  0. Back\n")

        choice = input("Choose (0-3): ").strip()
        if choice == "0":
            return
        if choice == "1":
            scrape_genres(site_url)
        elif choice == "2":
            show_stats(site_url)
        elif choice == "3":
            export_report(site_url)
        else:
            print("✗ Invalid choice. Please enter a number between 0 and 3.")
