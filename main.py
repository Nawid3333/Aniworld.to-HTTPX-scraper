#!/usr/bin/env python3
"""
AniWorld.to Anime Scraper and Index Manager (HTTPX version)
Automatically scrapes your watched anime from aniworld.to and maintains a local index.
No browser needed — uses httpx async HTTP client.
"""

import asyncio
import contextlib
import copy
import json
import logging
import logging.handlers
import os
import shutil
import sys
import time
from datetime import datetime
from urllib.parse import urlparse

from config.config import (
    DATA_DIR,
    DEFAULT_BATCH_FILE,
    EMAIL,
    LOG_FILE,
    PASSWORD,
    SERIES_INDEX_FILE,
    SITE_URL,
    SITE_URLS,
    configure_console,
)
from src.index_manager import (
    IndexManager,
    _extract_slug_from_field,
    _is_valid_series_url,
    confirm_and_save_changes,
    get_episode_counts,
    remove_series_from_index,
    show_vanished_series,
)
from src.scraper import AniWorldScraper, ScrapingPausedError

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))


# Global active site URL (set by domain probing)
ACTIVE_SITE_URL: str | None = None


def _host_label(site_url):
    """Return the hostname part of a URL for display."""
    return urlparse(site_url).netloc


def _format_host_rows(hosts):
    """Return a list of table-formatted host status lines.

    hosts is a list of (label, status, count, idx_count, compare_txt) tuples.
    """
    if not hosts:
        return []

    term_w = max(shutil.get_terminal_size().columns, 80)
    arrow_gap = "  "

    labels = ["Host", "Status", "Series", "Index", "Compare"]
    cols = {
        "host": max([len(str(label)) for label, *_ in hosts] + [len(labels[0])]),
        "status": max([len("OK" if status else "FAILED") for _, status, *_ in hosts] + [len(labels[1])]),
        "series": max([len(f"{count:,}") if count is not None else 1 for _, _, count, *_ in hosts] + [len(labels[2])]),
        "index": max(
            [len(f"{idx_count:,}") if idx_count is not None else 1 for _, _, _, idx_count, *_ in hosts]
            + [len(labels[3])]
        ),
        "compare": max(
            [len(str(compare_txt)) if compare_txt is not None else 1 for *_, compare_txt in hosts] + [len(labels[4])]
        ),
    }

    total = sum(cols.values()) + len(labels) * len(arrow_gap)
    if total > term_w:
        excess = total - term_w
        trimmable = cols["host"] - len(labels[0]) + cols["compare"] - len(labels[4])
        if trimmable > 0:
            factor = min(excess / trimmable, 1.0)
            cols["host"] = max(
                len(labels[0]),
                int(cols["host"] - (cols["host"] - len(labels[0])) * factor),
            )
            cols["compare"] = max(
                len(labels[4]),
                int(cols["compare"] - (cols["compare"] - len(labels[4])) * factor),
            )

    def _trunc(text, width):
        text = str(text)
        return text if len(text) <= width else text[: width - 1] + "…"

    sep_parts = ["─" * cols["host"]] + ["─" * cols[key] for key in ["status", "series", "index", "compare"]]

    lines = [
        arrow_gap
        + "  ".join(
            [
                f"{_trunc(labels[0], cols['host']):<{cols['host']}}",
                f"{labels[1]:<{cols['status']}}",
                f"{labels[2]:<{cols['series']}}",
                f"{labels[3]:<{cols['index']}}",
                f"{labels[4]:<{cols['compare']}}",
            ]
        ),
        arrow_gap + "  ".join(sep_parts),
    ]

    for label, status, count, idx_count, compare_txt in hosts:
        status_txt = "OK" if status else "FAILED"
        count_txt = f"{count:,}" if count is not None else "-"
        idx_txt = f"{idx_count:,}" if idx_count is not None else "-"
        cmp_txt = compare_txt if compare_txt is not None else "-"
        lines.append(
            arrow_gap
            + "  ".join(
                [
                    f"{_trunc(label, cols['host']):<{cols['host']}}",
                    f"{status_txt:<{cols['status']}}",
                    f"{count_txt:<{cols['series']}}",
                    f"{idx_txt:<{cols['index']}}",
                    f"{_trunc(cmp_txt, cols['compare']):<{cols['compare']}}",
                ]
            )
        )
    return lines


def _probe_hosts(scraper, site_urls):
    """Return probe results; on failure, print the error and return an empty list."""
    try:
        return asyncio.run(scraper.probe_sites(site_urls))
    except Exception as exc:
        print(f"  ✗ Probe failed: {exc}")
        logger.exception("Host probe failed")
        return []


def _fetch_catalogue_info_for_host(scraper, site_url):
    """Fetch count and slug set in one login/catalogue pass; return (count, slugs)."""
    try:
        return asyncio.run(scraper.get_catalogue_info_for_site(site_url))
    except Exception as exc:
        logger.warning("Could not fetch catalogue info for %s: %s", site_url, exc)
        return None, set()


def _collect_index_slugs(idx_mgr):
    """Collect slugs from the local index, including entries without a slug."""
    index_slugs_list = []
    index_entries_without_slug = []
    for title, s in idx_mgr.series_index.items():
        slug = _extract_slug_from_field(s.get("url", "") or s.get("link", ""))
        if slug:
            index_slugs_list.append(slug)
        else:
            index_entries_without_slug.append(
                {
                    "title": s.get("title", title),
                    "link": s.get("link", ""),
                    "url": s.get("url", ""),
                }
            )

    from collections import Counter

    index_duplicates = {slug: n for slug, n in Counter(index_slugs_list).items() if n > 1}
    return set(index_slugs_list), index_duplicates, index_entries_without_slug


def _save_combined_mismatch_report(report_path, idx_mgr, host_reports):
    """Write a single combined mismatch report covering all probed hosts."""
    index_slugs, index_duplicates, index_entries_without_slug = _collect_index_slugs(idx_mgr)
    report = {
        "generated": datetime.now().isoformat(),
        "index_count": len(idx_mgr.series_index),
        "index_unique_slugs": len(index_slugs),
        "index_entries_without_slug_count": len(index_entries_without_slug),
        "index_entries_without_slug": index_entries_without_slug,
        "index_duplicates": index_duplicates,
        "hosts": host_reports,
    }
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    in_only_set = set().union(*(set(h.get("only_in_index", [])) for h in host_reports))
    on_only_set = set().union(*(set(h.get("only_on_site", [])) for h in host_reports))
    dup_count = len(index_duplicates)
    has_mismatch = in_only_set or on_only_set or dup_count
    if has_mismatch:
        print(
            f"\n  Mismatch report: {len(host_reports)} host | in-index: {len(in_only_set)} | "
            f"on-site: {len(on_only_set)} | dups: {dup_count}"
        )
    logger.debug(
        "Combined mismatch report saved: %d hosts, %d only-in-index unique, "
        "%d only-on-site unique, %d index duplicates",
        len(host_reports),
        len(in_only_set),
        len(on_only_set),
        dup_count,
    )


def _remove_duplicate_index_entries(idx_mgr, index_duplicates):
    """Prompt to remove duplicate index entries and save if confirmed."""
    dup_extra = sum(index_duplicates.values()) - len(index_duplicates)
    print(f"    ⚠ Found {len(index_duplicates)} duplicate slug(s) in index (extra count: {dup_extra})")
    print("    Duplicate slugs:")
    for dup_slug in sorted(index_duplicates):
        print(f"      - {dup_slug}")

    choice = input("\nDelete duplicate index entries? (y/n): ").strip().lower()
    if choice != "y":
        return

    removed_titles = []
    for title, series in list(idx_mgr.series_index.items()):
        slug = _extract_slug_from_field(series.get("url", "") or series.get("link", ""))
        if slug in index_duplicates:
            removed_titles.append(title)
            del idx_mgr.series_index[title]
    idx_mgr.save_index()
    print(f"    🗑 Removed {len(removed_titles)} duplicate entries. Run 'Fetch new series' to rescrape them.")
    logger.info(
        "Removed %d duplicate index entries: %s",
        len(removed_titles),
        sorted(index_duplicates),
    )


def _cross_check_index(scraper, site_url, count, idx_mgr=None, site_slugs=None):
    """Compare site slugs against the local index for one host.

    idx_mgr can be passed in to avoid reloading the index repeatedly.
    site_slugs can be passed in if already fetched by the caller.

    Returns a tuple (idx_count, compare_txt, report_entry):
      - idx_count: number of entries in the local index, or None.
      - compare_txt: short status string for the host table.
      - report_entry: dict with per-host mismatch details (or None if skipped).
    """
    if idx_mgr is None:
        idx_mgr = IndexManager(SERIES_INDEX_FILE)
    idx_count = len(idx_mgr.series_index)
    if idx_count == 0:
        return None, None, None

    diff = idx_count - count
    if diff == 0:
        return (
            idx_count,
            "match",
            {
                "host": site_url,
                "site_count": count,
                "site_unique_slugs": count,
                "only_in_index": [],
                "only_on_site": [],
                "compare": "match",
            },
        )

    sign = "+" if diff > 0 else ""
    compare_txt = f"mismatch ({sign}{diff})"

    if site_slugs is None:
        logger.warning("Cannot compare slugs because site slug list is unavailable.")
        return (
            idx_count,
            compare_txt,
            {
                "host": site_url,
                "site_count": count,
                "site_unique_slugs": None,
                "only_in_index": [],
                "only_on_site": [],
                "compare": compare_txt,
            },
        )

    index_slugs, index_duplicates, index_entries_without_slug = _collect_index_slugs(idx_mgr)
    only_in_index = sorted(index_slugs - site_slugs)
    only_on_site = sorted(site_slugs - index_slugs)

    report_entry = {
        "host": site_url,
        "site_count": count,
        "site_unique_slugs": len(site_slugs),
        "only_in_index": only_in_index,
        "only_on_site": only_on_site,
        "compare": compare_txt,
    }

    return idx_count, compare_txt, report_entry


def _probe_sites_before_scrape(scraper, idx_mgr=None):
    """Probe configured hosts, show OK/FAILED, and set active site URL.

    For AniWorld.to, we only have a single SITE_URL (no fallback).
    This function checks if the primary URL is reachable and optionally
    fetches series count for display.
    """
    global ACTIVE_SITE_URL  # pylint: disable=global-statement

    site_urls = SITE_URLS if SITE_URLS else [SITE_URL]
    if not site_urls:
        scraper.site_url = SITE_URL
        ACTIVE_SITE_URL = SITE_URL
        return SITE_URL

    # Load the index once and reuse it for every host cross-check.
    if idx_mgr is None:
        idx_mgr = IndexManager(SERIES_INDEX_FILE)

    print("\n→ Checking host availability...\n")
    results = _probe_hosts(scraper, site_urls)

    ok_hosts = []
    host_counts = {}
    table_rows = []
    host_reports = []

    for site_url in site_urls:
        entry = next((r for r in results if r.get("site_url") == site_url), {})
        label = _host_label(site_url)
        ok = bool(entry.get("ok"))
        count = None
        idx_count = None
        compare_txt = None

        if ok:
            ok_hosts.append(site_url)
            count, site_slugs = _fetch_catalogue_info_for_host(scraper, site_url)
            host_counts[site_url] = count
            if count is not None:
                idx_count, compare_txt, report_entry = _cross_check_index(
                    scraper, site_url, count, idx_mgr=idx_mgr, site_slugs=site_slugs
                )
                if report_entry:
                    host_reports.append(report_entry)

        table_rows.append((label, ok, count, idx_count, compare_txt))

    for line in _format_host_rows(table_rows):
        print(line)

    # Write a single combined mismatch report covering all reachable hosts.
    if host_reports:
        report_path = os.path.join(DATA_DIR, "mismatch_report.json")
        _save_combined_mismatch_report(report_path, idx_mgr, host_reports)

    # Handle duplicate index entries once after all hosts have been checked.
    _, index_duplicates, _ = _collect_index_slugs(idx_mgr)
    if index_duplicates:
        _remove_duplicate_index_entries(idx_mgr, index_duplicates)

    if ok_hosts:
        scraper.site_url = ok_hosts[0]
        print(f"\n→ Active host: {scraper.site_url}")
        if scraper.site_url.startswith("http://"):
            print("  ⚠ WARNING: Active host is unencrypted (HTTP) — credentials sent in cleartext.")
    else:
        scraper.site_url = SITE_URL
        print(f"\n→ Active host: {scraper.site_url} (default)")

    if len(ok_hosts) >= 2:
        counts = [host_counts.get(host) for host in ok_hosts if host_counts.get(host) is not None]
        if len(counts) == len(ok_hosts) and counts:
            match = all(count == counts[0] for count in counts[1:])
            print(f"→ Cross-host counts: match = {match}")
        else:
            print("→ Cross-host counts: match = False")

    ACTIVE_SITE_URL = scraper.site_url
    return scraper.site_url


_MODE_LABELS = {
    "all_series": "Scrape all anime (option 1)",
    "new_only": "Scrape NEW anime only (option 2)",
    "unwatched": "Scrape unwatched anime (option 3)",
    "batch": "Batch add (option 5)",
    "single": "Single anime scrape",
    "subscribed": "Subscribed anime (option 7)",
    "watchlist": "Watchlist anime (option 7)",
    "both": "Subscribed+Watchlist anime (option 7)",
    "retry": "Retry failed (option 6)",
}


def _check_checkpoint(expected_mode):
    """Check for an existing checkpoint and prompt the user to resume or discard."""
    checkpoint_mode = AniWorldScraper.get_checkpoint_mode(DATA_DIR)
    if checkpoint_mode is None:
        return {"ok": True, "resume": False}

    checkpoint_file = os.path.join(DATA_DIR, ".scrape_checkpoint.json")
    saved_label = _MODE_LABELS.get(checkpoint_mode, checkpoint_mode)
    expected_label = _MODE_LABELS.get(expected_mode, expected_mode)

    if checkpoint_mode == expected_mode:
        print(f'\n⚠ Checkpoint found from a previous "{saved_label}" run!\n')
        choice = input("Resume from checkpoint? (y/n): ").strip().lower()
        if choice == "y":
            return {"ok": True, "resume": True}
        discard = input("Discard old checkpoint and start fresh? (y/n): ").strip().lower()
        if discard == "y":
            with contextlib.suppress(OSError):
                os.remove(checkpoint_file)
            return {"ok": True, "resume": False}
        return {"ok": False, "resume": False}

    print(f'\n⚠ A checkpoint exists from a different mode: "{saved_label}"')
    print(f'   You are about to run: "{expected_label}"\n')
    discard = input("Discard the old checkpoint and continue? (y/n): ").strip().lower()
    if discard == "y":
        with contextlib.suppress(OSError):
            os.remove(checkpoint_file)
        return {"ok": True, "resume": False}
    return {"ok": False, "resume": False}


# Configure logging with rotation
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.ERROR)

logger = logging.getLogger(__name__)


def _extract_slug(entry):
    """Extract anime slug from a dict entry using link (primary) or url (fallback)."""
    if not isinstance(entry, dict):
        return None
    slug = _extract_slug_from_field(entry.get("link", ""))
    if slug:
        return slug
    slug = _extract_slug_from_field(entry.get("url", ""))
    if slug:
        title = entry.get("title", "?")
        print(f"  ⚠ Used URL fallback for slug extraction: {title}")
        logger.warning("Used URL fallback for slug extraction: %s", title)
        return slug
    return None


def print_header():
    print("=" * 60)
    print("  ANIWORLD.TO ANIME SCRAPER & INDEX MANAGER (httpx)")
    print("=" * 60 + "\n")


def print_completed_series_alerts(index_manager=None, allow_rescrape=True):
    """Alert user about series that need attention:
    1. Fully watched but not subscribed
    2. Ongoing (started but incomplete) but not on watchlist

    When allow_rescrape is False, the rescrape prompt is suppressed
    (used to prevent recursive prompts during a rescrape).
    """
    try:
        if index_manager is None:
            index_manager = IndexManager(SERIES_INDEX_FILE)

        if not index_manager.series_index:
            return

        completed_not_sub = []
        ongoing_no_wl = []

        for s in index_manager.series_index.values():
            total, watched = get_episode_counts(s)
            subscribed = s.get("subscribed", False)
            watchlist = s.get("watchlist", False)

            if total > 0 and watched == total and not subscribed:
                completed_not_sub.append(s)
            elif total > 0 and 0 < watched < total and not watchlist:
                ongoing_no_wl.append(s)

        if completed_not_sub:
            completed_not_sub.sort(key=lambda s: s.get("title", ""))
            print("\n" + "⚠" * 35)
            print(f"⚠ {len(completed_not_sub)} COMPLETED ANIME — NOT SUBSCRIBED:")
            print("─" * 70)
            for s in completed_not_sub:
                print(f"  • {s.get('title')}")
            print("─" * 70)
            print("  Consider subscribing or leaving as-is.")
            print("⚠" * 35)

            if allow_rescrape:
                rescrape = input("\nRescrape these anime to update Sub/WL status? (y/n): ").strip().lower()
                if rescrape == "y":
                    urls = [s.get("url") for s in completed_not_sub if s.get("url")]
                    if not urls:
                        print("✗ No URLs found for these anime")
                    else:
                        print(f"\n→ Rescraping {len(urls)} completed anime...")
                        _run_scrape_and_save(
                            run_kwargs={"url_list": urls, "parallel": False},
                            description=f"Rescrape completed anime ({len(urls)})",
                            success_msg=f"Rescrape completed! {len(urls)} anime updated.",
                            no_data_msg="No data scraped",
                            post_scrape_allow_rescrape=False,
                        )

        if ongoing_no_wl:
            ongoing_no_wl.sort(key=lambda s: s.get("title", ""))
            print("\n" + "⚠" * 35)
            print(f"⚠ {len(ongoing_no_wl)} ONGOING ANIME — NOT ON WATCHLIST:")
            print("─" * 70)
            for s in ongoing_no_wl:
                print(f"  • {s.get('title')}")
            print("─" * 70)
            print("  Consider adding them to your watchlist.")
            print("⚠" * 35)

    except Exception as e:
        logger.error("Error printing series alerts: %s", e)


def check_disk_space(min_mb=100):
    """Check if enough disk space is available."""
    try:
        stat = shutil.disk_usage(DATA_DIR)
        available_mb = stat.free / (1024 * 1024)
        if available_mb < min_mb:
            print("\n✗ WARNING: Low disk space!")
            print(f"  Available: {available_mb:.1f} MB (minimum needed: {min_mb} MB)")
            print("  Please free up disk space before scraping.\n")
            return False
        return True
    except Exception as e:
        logger.warning("Could not check disk space: %s", e)
        return True


def validate_credentials():
    """Check that credentials are configured."""
    if not EMAIL or not PASSWORD:
        print("\n✗ ERROR: Credentials not configured!")
        print("\nPlease follow these steps:")
        print("1. Create a '.env' file in the config/ directory")
        print("2. Add your aniworld.to email and password:")
        print("   ANIWORLD_EMAIL=your_email@example.com")
        print("   ANIWORLD_PASSWORD=your_password")
        print("3. Save the file and try again\n")
        return False
    return True


def show_menu():
    print("\nOptions:")
    print("  1. Scrape all anime")
    print("  2. Scrape only NEW anime")
    print("  3. Scrape unwatched anime")
    print("  4. Generate report")
    print("  5. Single link / batch add")
    print("  6. Retry failed scrapes")
    print("  7. Scrape subscribed/watchlist anime")
    print("  0. Exit\n")


def _run_scrape_and_save(
    run_kwargs,
    description,
    success_msg,
    no_data_msg,
    pre_save_hook=None,
    vanished_scope=None,
    post_scrape_allow_rescrape=True,
):
    """Common pattern: create scraper, run, confirm & save, handle errors.

    Args:
        pre_save_hook: Optional callable(scraper, pre_index) called after scraping
                       but before confirm_and_save. Can modify scraper.series_data.
        vanished_scope: Override scope for show_vanished_series (default: auto-detect).
        post_scrape_allow_rescrape: If False, suppress the rescrape prompt
                                    after the scrape (used during recursive rescrapes).
    """
    pre_index = IndexManager(SERIES_INDEX_FILE) if pre_save_hook else None
    t_start = time.perf_counter()
    try:
        scraper = AniWorldScraper()
        if ACTIVE_SITE_URL:
            scraper.site_url = ACTIVE_SITE_URL
        else:
            _probe_sites_before_scrape(scraper)
        scraper.run(**run_kwargs)

        if scraper.series_data:
            if pre_save_hook:
                pre_save_hook(scraper, pre_index)

            index_manager = IndexManager(SERIES_INDEX_FILE)

            if scraper.all_discovered_series is not None:
                all_slugs = {_extract_slug(s) for s in scraper.all_discovered_series} - {None}
                scope = vanished_scope or ("new_only" if run_kwargs.get("new_only") else "all")
                show_vanished_series(
                    index_manager.series_index,
                    all_slugs,
                    scope,
                    index_file=SERIES_INDEX_FILE,
                    new_data=scraper.series_data,
                    scraper=scraper,
                )
                # Always reload: show_vanished_series may have deleted entries from disk
                index_manager.load_index()

            result = confirm_and_save_changes(
                scraper.series_data,
                description,
                index_manager,
                active_site_url=ACTIVE_SITE_URL,
            )
            if isinstance(result, dict) and result.get("rescrape"):
                # User already confirmed deletion in the integrity dialog — proceed directly
                n = len(result["urls"])
                print(f"\n→ Deleting {n} critical series from index before rescraping...")
                remove_series_from_index(SERIES_INDEX_FILE, result["titles"])
                print(f"\n→ Rescraping {n} critical series...\n")
                _run_scrape_and_save(
                    run_kwargs={"url_list": result["urls"], "parallel": False},
                    description=f"Rescrape critical series ({n})",
                    success_msg=f"Critical series rescraping completed! {n} series updated.",
                    no_data_msg="No data scraped for critical series",
                    post_scrape_allow_rescrape=False,
                )
            elif result:
                print(f"\n✓ {success_msg}")
                print_completed_series_alerts(index_manager, allow_rescrape=post_scrape_allow_rescrape)
                logger.info(success_msg)
                # Final cross-check: scraped count vs index count
                # Final cross-check only meaningful for full catalog scrapes.
                if scraper.all_discovered_series is not None and not run_kwargs.get("new_only"):
                    scraped_count = len(scraper.series_data)
                    idx_count = len(index_manager.series_index)
                    if scraped_count == idx_count:
                        print(f"  Index count: {idx_count}  →  match = True")
                    else:
                        diff = scraped_count - idx_count
                        sign = "+" if diff > 0 else ""
                        print(f"  Index count: {idx_count}  →  match = False ({sign}{diff} difference)")
                        print("  → Vanished/renamed series were already checked above.")
        else:
            if run_kwargs.get("retry_failed") and scraper.failed_links:
                n = len(scraper.failed_links)
                print(f"\n✗ All {n} retried anime failed again:")
                for entry in scraper.failed_links:
                    title = entry.get("title") or entry.get("url", "?")
                    reason = entry.get("reason", "unknown error")
                    print(f"  • {title}  →  {reason}")
                print("\n→ Failed list preserved. Use option 6 to retry again.")
                logger.warning("All %d retried anime failed again", n)
            else:
                print(f"\n⚠ {no_data_msg}")
                logger.warning(no_data_msg)

        # Only clear checkpoint if scraping completed (not paused)
        if not scraper.paused:
            scraper.clear_checkpoint()
        else:
            print("\n⚠ Scraping was paused — checkpoint preserved for resume.")

        if scraper.failed_links:
            print(f"\n⚠ {len(scraper.failed_links)} anime failed during scraping.")
            print("→ Use option 6 (Retry failed series) to rescrape these later.")

        t_elapsed = time.perf_counter() - t_start
        print(f"\n⏱ Scrape duration: {t_elapsed / 60:.1f}m ({t_elapsed:.1f}s)")

        return scraper
    except ScrapingPausedError:
        # Scraper has already saved the checkpoint when paused.
        print("\n⚠ Scraping was paused — checkpoint preserved for resume.")
        logger.info("%s paused — returning to menu", description)
        if scraper is not None and scraper.failed_links:
            print(f"\n⚠ {len(scraper.failed_links)} anime failed.")
            print("→ Use option 6 (Retry failed series) to rescrape these later.")
        return scraper
    except (KeyboardInterrupt, SystemExit):
        print("\n⚠ Scraping interrupted by Ctrl+C")
        if "scraper" in locals() and scraper.series_data:
            index_manager = IndexManager(SERIES_INDEX_FILE)
            result = confirm_and_save_changes(
                scraper.series_data,
                description,
                index_manager,
                active_site_url=ACTIVE_SITE_URL,
            )
            if isinstance(result, dict) and result.get("rescrape"):
                remove_series_from_index(SERIES_INDEX_FILE, result["titles"])
                for url, title in zip(result["urls"], result["titles"], strict=False):
                    scraper.failed_links.append(
                        {
                            "url": url,
                            "title": title,
                            "link": "",
                            "reason": "integrity_check_failed",
                        }
                    )
                scraper.save_failed_series()
                print(f"\n✓ {len(result['urls'])} critical series removed from index and added to retry list.")
                print("→ Use option 6 (Retry failed series) to rescrape these.")
                logger.info("Critical series removed from index and added to retry list after Ctrl+C")
            elif result:
                print(f"\n✓ Partial data saved ({len(scraper.series_data)} series)")
                logger.info("%s interrupted — partial data saved", description)
        if "scraper" in locals() and scraper.failed_links:
            print(f"\n⚠ {len(scraper.failed_links)} anime failed.")
            print("→ Use option 6 (Retry failed series) to rescrape these later.")
        return scraper if "scraper" in locals() else None
    except OSError as e:
        print(f"\n✗ Network error occurred: {str(e)}")
        logger.error("Network error in %s: %s", description, e)
    except Exception as e:
        print(f"\n✗ Unexpected error: {str(e)}")
        logger.error("Unexpected error in %s: %s", description, e)
    return None


def scrape_all_series():
    """Scrape all anime with sequential or parallel mode selection"""
    print("\n→ Starting AniWorld.to complete scraper (httpx)...\n")

    chk = _check_checkpoint("all_series")
    if not chk["ok"]:
        print("✗ Cancelled")
        return
    resume = chk["resume"]

    print("\nScraping mode:")
    print("  1. Sequential (slower, but most reliable)")
    print("  2. Parallel (faster, uses multiple sessions)")
    print("  0. Back\n")
    mode_choice = input("Choose mode (0-2) [default: 2]: ").strip() or "2"

    if mode_choice == "0":
        return
    if mode_choice not in ["1", "2"]:
        print("⚠ Invalid choice, using default (parallel)")
        use_parallel = True
    else:
        use_parallel = mode_choice == "2"

    _run_scrape_and_save(
        run_kwargs={"resume_only": resume, "parallel": use_parallel},
        description="All anime scrape",
        success_msg="Scraping completed and saved!",
        no_data_msg="No anime data scraped",
    )


def scrape_new_series():
    """Scrape only new anime not yet in the index"""
    print("\n→ Starting AniWorld.to scraper (NEW anime only)...\n")

    chk = _check_checkpoint("new_only")
    if not chk["ok"]:
        print("✗ Cancelled")
        return
    resume = chk["resume"]

    _run_scrape_and_save(
        run_kwargs={"new_only": True, "resume_only": resume},
        description="New anime data",
        success_msg="New anime scraping completed successfully!",
        no_data_msg="No new anime found",
    )


def scrape_unwatched():
    """Scrape only unwatched/ongoing/unstarted anime from the existing index."""
    print("\n→ Scrape unwatched anime (skipping fully watched)...\n")

    index_manager = IndexManager(SERIES_INDEX_FILE)
    if not index_manager.series_index:
        print("✗ No anime in index. Run a full scrape first (option 1).")
        return

    unwatched_urls = []
    skipped = 0
    for series in index_manager.series_index.values():
        total, watched = get_episode_counts(series)
        if total > 0 and watched >= total:
            skipped += 1
        else:
            url = series.get("url")
            if url:
                unwatched_urls.append(url)

    if not unwatched_urls:
        print("✓ All anime are fully watched! Nothing to scrape.")
        return

    print(f"  Found {len(unwatched_urls)} unwatched/ongoing anime (skipping {skipped} fully watched)\n")

    chk = _check_checkpoint("unwatched")
    if not chk["ok"]:
        print("✗ Cancelled")
        return
    resume = chk["resume"]

    print("\nScraping mode:")
    print("  1. Sequential (slower, but most reliable)")
    print("  2. Parallel (faster, uses multiple sessions)")
    print("  0. Back\n")
    mode_choice = input("Choose mode (0-2) [default: 2]: ").strip() or "2"

    if mode_choice == "0":
        return
    if mode_choice not in ["1", "2"]:
        print("⚠ Invalid choice, using default (parallel)")
        use_parallel = True
    else:
        use_parallel = mode_choice == "2"

    _run_scrape_and_save(
        run_kwargs={
            "url_list": unwatched_urls,
            "resume_only": resume,
            "checkpoint_mode": "unwatched",
            "parallel": use_parallel,
        },
        description=f"Unwatched anime scrape ({len(unwatched_urls)} anime)",
        success_msg=f"Unwatched anime scraping completed! ({len(unwatched_urls)} anime)",
        no_data_msg="No data scraped",
    )


def single_or_batch_add():
    """Add single anime by URL or batch from file with auto-detect"""
    default_file = DEFAULT_BATCH_FILE
    print("\n→ Add single link / batch from file")
    print("  • Paste URL → scrapes single anime")
    print("  • Enter filename → uses that file for batch")
    print(f"  • Press Enter → uses default ({default_file})")
    print("  • Type 0   → back to main menu\n")

    user_input = input(f"Enter [default: {default_file}]: ").strip()

    if user_input == "0":
        return
    if not user_input:
        user_input = default_file

    if user_input.startswith(("http://", "https://")):
        add_single_series(user_input)
    else:
        if not os.path.exists(user_input):
            print(f"✗ File not found: {user_input}")
            return
        batch_add_from_file(user_input)


def add_single_series(url):
    """Add a single anime to the index by URL"""
    print(f"\n→ Scraping single anime: {url}\n")

    if not _is_valid_series_url(url):
        print("✗ Invalid aniworld.to anime URL format")
        return

    _run_scrape_and_save(
        run_kwargs={"single_url": url, "parallel": False},
        description="Single anime",
        success_msg="Anime added/updated successfully!",
        no_data_msg="No data scraped for this anime",
    )


def batch_add_from_file(file_path):
    """Add multiple anime from a text file containing URLs"""
    try:
        urls = []
        skipped = []
        with open(file_path, encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                url = line.strip()
                if not url or url.startswith("#"):
                    continue
                parsed = urlparse(url)
                if parsed.scheme and parsed.scheme not in ("http", "https"):
                    skipped.append((line_num, url))
                    continue
                if not _is_valid_series_url(url):
                    skipped.append((line_num, url))
                    continue
                urls.append(url)
        if skipped:
            print(f"⚠ Skipped {len(skipped)} invalid URL(s):")
            for line_num, bad_url in skipped[:5]:
                print(f"  Line {line_num}: {bad_url[:80]}")
            if len(skipped) > 5:
                print(f"  ... and {len(skipped) - 5} more")
    except Exception as e:
        print(f"✗ Failed to read file: {str(e)}")
        logger.error("Failed to read file %s: %s", file_path, e)
        return

    if not urls:
        print("✗ No valid URLs found in file")
        return

    print(f"✓ Found {len(urls)} valid URL(s) in file\n")
    print("URLs to process:")
    for url in urls[:5]:
        print(f"  • {url}")
    if len(urls) > 5:
        print(f"  ... and {len(urls) - 5} more")

    confirm = input("\nProceed with batch add? (y/n): ").strip().lower()
    if confirm != "y":
        print("✗ Cancelled")
        return

    chk = _check_checkpoint("batch")
    if not chk["ok"]:
        print("✗ Cancelled")
        return
    resume = chk["resume"]

    print(f"\n→ Starting batch scraper for {len(urls)} anime...\n")

    run_kwargs = {"url_list": urls, "resume_only": resume, "parallel": True}

    _run_scrape_and_save(
        run_kwargs=run_kwargs,
        description=f"Batch add ({len(urls)} anime)",
        success_msg=f"Batch add completed! {len(urls)} anime processed.",
        no_data_msg="No data scraped",
    )


def _report_batch_export(added, skipped, urls_file, noun):
    """Print what the automatic batch-file export actually did.

    The export runs without asking now, so the terminal has to say plainly
    what changed -- which URLs were added, and how many were already there.
    """
    if added:
        print(f"\n✓ Added {len(added)} {noun} URL(s) to {urls_file}:")
        for url in added[:10]:
            print(f"    + {url}")
        if len(added) > 10:
            print(f"    ... and {len(added) - 10} more")
    else:
        print(f"\n✓ {urls_file} already lists every ongoing {noun} — nothing added")
    if skipped:
        print(f"  ({skipped} already listed)")
    print(f"  → Use option 5 (Single link / batch add) to rescrape these {noun}")
    print("  (existing entries are kept — delete the file first for a clean replace)")


def _append_urls_to_batch_file(urls_file, urls):
    """Add URLs to the batch file without discarding what is already there.

    Exporting used to open the file in "w" mode, which replaced the whole
    file -- a hand-curated list, comments and all, vanished the moment
    someone answered yes to the export prompt. Appending keeps that work.

    A URL already present is skipped, including one that is commented out:
    commenting a line was a deliberate decision to skip that series, and an
    export should not quietly undo it. To start clean, delete the file and
    export again.

    Returns (added_urls, skipped_count).
    """
    existing_lines = []
    known = set()
    if os.path.exists(urls_file):
        with open(urls_file, encoding="utf-8") as fh:
            existing_lines = fh.read().splitlines()
        for line in existing_lines:
            stripped = line.strip().lstrip("#").strip()
            if stripped:
                known.add(stripped)

    fresh = [u for u in urls if u not in known]
    if fresh:
        body = list(existing_lines)
        while body and not body[-1].strip():
            body.pop()
        body.extend(fresh)
        with open(urls_file, "w", encoding="utf-8") as fh:
            fh.write("\n".join(body) + "\n")
    return fresh, len(urls) - len(fresh)


def _show_ongoing_and_export(report, index_manager):
    """Show ongoing anime and offer to export their URLs to series_urls.txt"""
    ongoing_count = report["categories"]["ongoing"]["count"]
    if ongoing_count == 0:
        return

    print("\n  ONGOING ANIME")
    ongoing_titles = report["categories"]["ongoing"]["titles"]
    idx_w = len(str(min(ongoing_count, 10)))
    title_w = max((len(t) for t in ongoing_titles[:10]), default=0)
    print(f"    {'#':<{idx_w}}  {'Title':<{title_w}}")
    print(f"    {'─' * idx_w}  {'─' * title_w}")
    for i, title in enumerate(ongoing_titles[:10], 1):
        row = f"    {i:<{idx_w}}  {title:<{title_w}}"
        print(row.rstrip())
    if ongoing_count > 10:
        print(f"    ... and {ongoing_count - 10} more")

    # Exported automatically: appending is additive and de-duplicated, so
    # there is nothing to lose by doing it, and the old prompt only stood
    # between the report and a batch file that should already be current.
    try:
        urls = []
        for title in ongoing_titles:
            series_data = index_manager.series_index.get(title, {})
            url = series_data.get("url") or series_data.get("link")
            if url:
                if not url.startswith("http"):
                    url = f"https://aniworld.to{url}"
                urls.append(url)

        if not urls:
            print("\n⚠ Could not extract URLs from ongoing anime")
            return

        urls_file = DEFAULT_BATCH_FILE
        added, skipped = _append_urls_to_batch_file(urls_file, urls)
        _report_batch_export(added, skipped, urls_file, "anime")
        logger.info(
            "Appended %d URLs to %s (%d already present)",
            len(added),
            urls_file,
            skipped,
        )
    except Exception as e:
        print(f"\n✗ Failed to export URLs: {str(e)}")
        logger.error("Failed to export URLs: %s", e)


def _print_report_summary(report, report_file, filter_name=None):
    """Print enhanced report summary to console."""
    stats = report["metadata"]["statistics"]
    ongoing_count = report["categories"]["ongoing"]["count"]
    not_started_count = report["categories"]["not_started"]["count"]
    not_started_sub_wl_count = report["categories"]["not_started_subscribed_watchlist"]["count"]
    waiting_count = report["categories"]["waiting_for_new_episodes"]["count"]

    header = f"REPORT SUMMARY ({filter_name.upper().replace('_', ' ')})" if filter_name else "REPORT SUMMARY"
    term_w = max(shutil.get_terminal_size().columns, 80)

    metrics = [
        ("Total anime", str(stats["total_series"])),
        ("Completed (100%)", str(stats.get("completed_count", stats["watched"]))),
        ("Ongoing (started)", str(stats.get("ongoing_count", ongoing_count))),
    ]
    if waiting_count > 0:
        metrics.append(("Waiting for new eps", str(waiting_count)))
    metrics.extend(
        [
            (
                "Not started (0%)",
                str(stats.get("not_started_count", not_started_count)),
            ),
            ("Not started (Sub/WL)", str(not_started_sub_wl_count)),
            ("Total episodes", str(stats["total_episodes"])),
            ("Watched episodes", str(stats["watched_episodes"])),
            ("Unwatched episodes", str(stats.get("unwatched_episodes", 0))),
            ("Avg episodes/anime", str(stats.get("average_episodes_per_series", 0))),
            ("Average completion", f"{stats['average_completion']:.1f}%"),
            ("Subscribed", str(stats.get("subscribed_count", 0))),
            ("Watchlist", str(stats.get("watchlist_count", 0))),
            ("Both (Sub+WL)", str(stats.get("both_subscribed_and_watchlist", 0))),
        ]
    )

    label_w = min(max((len(m[0]) for m in metrics), default=0), term_w // 2 - 4)
    value_w = min(max((len(m[1]) for m in metrics), default=0), term_w // 2 - 4)
    table_w = label_w + value_w + 3
    sep = "─" * table_w

    def _trunc(text, width):
        return text if len(text) <= width else text[: width - 1] + "…"

    print("\n" + sep)
    print(f"  {header}")
    print("  " + "─" * (table_w - 2))
    for label, value in metrics:
        line = f"  {_trunc(label, label_w):<{label_w}}  {_trunc(value, value_w):<{value_w}}"
        print(line.rstrip())
    print(sep)

    dist = stats.get("completion_distribution", {})
    if dist:
        print("\n  COMPLETION BREAKDOWN")
        bucket_w = max((len(k) for k in dist), default=0)
        count_w = max((len(str(v)) for v in dist.values()), default=0)
        print(f"    {'Bucket':<{bucket_w}}  {'Count':<{count_w}}")
        print(f"    {'─' * bucket_w}  {'─' * count_w}")
        for bucket, count in dist.items():
            row = f"    {bucket:<{bucket_w}}  {str(count):<{count_w}}"
            print(row.rstrip())

    most = stats.get("most_completed_series", [])
    if most:
        print("\n  MOST COMPLETED ONGOING")
        for i, s in enumerate(most, 1):
            print(f"    {i}. {s['title']} — {s['completion']:.1f}% ({s['progress']})")

    least = stats.get("least_completed_series", [])
    if least:
        print("\n  LEAST COMPLETED ONGOING")
        for i, s in enumerate(least, 1):
            print(f"    {i}. {s['title']} — {s['completion']:.1f}% ({s['progress']})")

    print("\n  SAVED TO")
    print(f"    {report_file}")
    print(sep + "\n")


def generate_report():
    """Generate anime report with optional filtering by subscription status"""
    print("\n→ Generate report")
    print("  1. Full report (all anime)")
    print("  2. Subscription/watchlist filtered report")
    print("  0. Back\n")

    choice = input("Choose report type (0-2): ").strip()

    if choice == "0":
        return

    try:
        index_manager = IndexManager(SERIES_INDEX_FILE)

        if choice == "1":
            print("\n→ Generating full report...")
            report = index_manager.get_full_report()
            report_file = os.path.join(DATA_DIR, "series_report.json")
            with open(report_file, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            _print_report_summary(report, report_file)
            logger.info("Full report generated")
            print_completed_series_alerts(index_manager)
            _show_ongoing_and_export(report, index_manager)

        elif choice == "2":
            print("\n→ Subscription/watchlist report")
            print("  1. Only subscribed")
            print("  2. Only watchlist")
            print("  3. Both")
            print("  0. Back\n")

            sub_choice = input("Choose filter (0-3): ").strip()

            if sub_choice == "0":
                return

            if sub_choice == "1":
                print("\n→ Generating report for subscribed anime...")
                report = index_manager.get_full_report(filter_subscribed=True, filter_watchlist=False)
                filter_name = "subscribed_only"
            elif sub_choice == "2":
                print("\n→ Generating report for watchlist anime...")
                report = index_manager.get_full_report(filter_subscribed=False, filter_watchlist=True)
                filter_name = "watchlist_only"
            elif sub_choice == "3":
                print("\n→ Generating report for subscribed AND watchlist...")
                report = index_manager.get_full_report(filter_subscribed=True, filter_watchlist=True)
                filter_name = "both_subscribed_watchlist"
            else:
                print("⚠ Invalid choice")
                return

            report_file = os.path.join(DATA_DIR, f"series_report_{filter_name}.json")
            with open(report_file, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            _print_report_summary(report, report_file, filter_name)
            logger.info("Filtered report generated: %s", filter_name)
            print_completed_series_alerts(index_manager)
            _show_ongoing_and_export(report, index_manager)

        else:
            print("⚠ Invalid choice")

    except Exception as e:
        print(f"\n✗ Error generating report: {str(e)}")
        logger.error("Error generating report: %s", e)


def _inject_disappeared_series(scraper, pre_index, source):
    """Inject stubs for series no longer on account pages so merge can prompt."""
    discovered_slugs = {_extract_slug(s) for s in (scraper.all_discovered_series or [])} - {None}
    failed_slugs = {_extract_slug(fl) for fl in scraper.failed_links if isinstance(fl, dict)} - {None}
    scraped_titles = {s.get("title") for s in scraper.series_data if s.get("title")}

    for field, sources in [
        ("watchlist", ("watchlist", "both")),
        ("subscribed", ("subscribed", "both")),
    ]:
        if source not in sources:
            continue
        injected = []
        for title, entry in pre_index.series_index.items():
            if not entry.get(field, False):
                continue
            slug = _extract_slug(entry)
            if not slug or slug in discovered_slugs or slug in failed_slugs:
                continue
            if title in scraped_titles:
                # Already scraped — just flip the flag
                for item in scraper.series_data:
                    if item.get("title") == title:
                        item[field] = False
                        break
            else:
                stub = copy.deepcopy(entry)
                stub[field] = False
                scraper.series_data.append(stub)
                scraped_titles.add(title)
            injected.append(title)
        if injected:
            print(f"\n  ⚠ {len(injected)} anime no longer {field} (will prompt for confirmation):")
            for name in injected:
                print(f"    • {name}")


def scrape_subscribed_watchlist():
    """Scrape subscribed/watchlist anime with disappeared-series detection."""
    print("\n→ Scrape subscribed/watchlist anime")
    print("  1. Only subscribed")
    print("  2. Only watchlist")
    print("  3. Both")
    print("  0. Back\n")

    sub_choice = input("Choose source (0-3) [default: 3]: ").strip() or "3"
    if sub_choice == "0":
        return
    source = {"1": "subscribed", "2": "watchlist"}.get(sub_choice, "both")

    chk = _check_checkpoint(source)
    if not chk["ok"]:
        print("✗ Cancelled")
        return

    def _hook(scraper, pre_index):
        _inject_disappeared_series(scraper, pre_index, source)

    _run_scrape_and_save(
        run_kwargs={"account_source": source, "resume_only": chk["resume"]},
        description="Account anime",
        success_msg="Account anime scraping completed!",
        no_data_msg="No anime found on account pages",
        pre_save_hook=_hook,
        vanished_scope=source,
    )


def retry_failed_series():
    """Retry previously failed anime in sequential mode"""
    print("\n→ Retry failed anime from last run\n")

    chk = _check_checkpoint("retry")
    if not chk["ok"]:
        print("✗ Cancelled")
        return
    resume = chk["resume"]

    temp_scraper = AniWorldScraper()
    failed_list = temp_scraper.load_failed_series()
    if not failed_list:
        print("✓ No failed anime found. Nothing to retry.")
        return
    print(f"✓ Found {len(failed_list)} failed anime from last run")
    print("\n→ Starting retry in sequential mode (for reliability)...")

    _run_scrape_and_save(
        run_kwargs={"retry_failed": True, "parallel": False, "resume_only": resume},
        description="Retry data",
        success_msg="Retry completed successfully!",
        no_data_msg="No data from retry",
    )


def main():
    """Main application loop"""
    # Already done at config import time; repeated here so the entry point
    # does not depend on that import side effect.
    configure_console()

    idx_mgr = IndexManager(SERIES_INDEX_FILE)
    index_count = len(idx_mgr.series_index)
    print(f"✓ Index loaded ({os.path.abspath(SERIES_INDEX_FILE)}) ({index_count:,} entries)\n")

    print_header()

    if not validate_credentials():
        sys.exit(1)

    print(f"\u2713 Credentials found for user: {EMAIL}\n")

    if not check_disk_space():
        response = input("Continue anyway? (y/n): ").strip().lower()
        if response != "y":
            sys.exit(1)

    scraper = AniWorldScraper()
    _probe_sites_before_scrape(scraper, idx_mgr=idx_mgr)

    while True:
        show_menu()
        choice = input("Enter your choice (0-7): ").strip()

        if not choice.isdigit() or not 0 <= int(choice) <= 7:
            print("✗ Invalid choice. Please enter a number between 0 and 7.")
            continue

        if choice in ["1", "2", "3", "5", "6", "7"] and not check_disk_space():
            print("⚠ Aborting due to low disk space.")
            continue

        if choice == "1":
            scrape_all_series()
        elif choice == "2":
            scrape_new_series()
        elif choice == "3":
            scrape_unwatched()
        elif choice == "4":
            generate_report()
        elif choice == "5":
            single_or_batch_add()
        elif choice == "6":
            retry_failed_series()
        elif choice == "7":
            scrape_subscribed_watchlist()
        elif choice == "0":
            print("\n✓ Goodbye!\n")
            break


if __name__ == "__main__":
    main()
