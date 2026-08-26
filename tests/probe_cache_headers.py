"""Probe whether conditional requests (ETag / Last-Modified) are usable here.

Read-only diagnostic. It logs in, fetches a handful of pages that this
scraper already fetches anyway, and reports what the server says about
caching. It writes nothing except its own report file, and never touches
the series index.

The question it answers is not "does the server send an ETag" but "can we
trust one". These pages carry *per-account* watched state, so an ETag that
is computed from the shared page body would stay identical while your watch
state changes underneath it. Skipping a page on that basis would silently
lose exactly the data this project exists to record, so the bar is high:

  1. Is an ETag or Last-Modified sent at all?
  2. Does a conditional re-request actually return 304?
  3. Is the value stable across two identical requests?
  4. Does the response Vary on Cookie, i.e. is it session-aware?

If any of those fails, conditional requests are not safe here and the
answer is simply to leave the scraper alone.

Run from the project root:

    python tests/probe_cache_headers.py [--count N]
"""

import argparse
import asyncio
import json
import re
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.config import SERIES_INDEX_FILE  # noqa: E402
from src.scraper import AniWorldScraper  # noqa: E402
from tests import fixture_spec  # noqa: E402

SCRAPER_CLS = AniWorldScraper
REPORT_FILE = Path(__file__).resolve().parent / "cache_probe_report.json"

CACHE_HEADERS = (
    "etag",
    "last-modified",
    "cache-control",
    "expires",
    "age",
    "vary",
    "x-cache",
    "cf-cache-status",
)


def pick_urls(limit):
    """Take a few real series URLs from the index. Read-only."""
    with open(SERIES_INDEX_FILE, encoding="utf-8") as fh:
        data = json.load(fh)
    items = data if isinstance(data, list) else list(data.values())
    urls = []
    for entry in items:
        url = entry.get("url") or entry.get("link") or ""
        if re.search(fixture_spec.SLUG_RE, url):
            urls.append(url)
        if len(urls) >= limit:
            break
    return urls


def headers_of(resp):
    return {k: v for k, v in resp.headers.items() if k.lower() in CACHE_HEADERS}


async def probe_one(client, url):
    """Fetch a URL three times: plain, plain again, then conditionally."""
    result = {"url": url}

    first = await client.get(url, follow_redirects=True)
    result["status_1"] = first.status_code
    result["headers_1"] = headers_of(first)

    second = await client.get(url, follow_redirects=True)
    result["status_2"] = second.status_code
    result["headers_2"] = headers_of(second)

    etag = first.headers.get("etag")
    last_mod = first.headers.get("last-modified")
    result["etag_stable"] = bool(etag) and etag == second.headers.get("etag")
    result["body_identical"] = first.text == second.text

    cond = {}
    if etag:
        cond["If-None-Match"] = etag
    if last_mod:
        cond["If-Modified-Since"] = last_mod
    if cond:
        third = await client.get(url, headers=cond, follow_redirects=True)
        result["conditional_sent"] = cond
        result["conditional_status"] = third.status_code
        result["got_304"] = third.status_code == 304
    else:
        result["conditional_sent"] = None
        result["conditional_status"] = None
        result["got_304"] = False

    return result


def verdict(results):
    """Decide, conservatively, whether conditional requests are usable."""
    if not results:
        return "NO DATA", ["nothing was probed"]

    reasons = []
    with_validator = [r for r in results if r["headers_1"].get("etag") or r["headers_1"].get("last-modified")]
    if not with_validator:
        return "NOT USABLE", ["the server sends neither ETag nor Last-Modified on these pages"]

    got_304 = [r for r in results if r["got_304"]]
    if not got_304:
        reasons.append("a conditional re-request never returned 304 — the server ignores it")

    unstable = [r for r in results if r["headers_1"].get("etag") and not r["etag_stable"]]
    if unstable:
        reasons.append(f"{len(unstable)}/{len(results)} pages changed their ETag between two identical requests")

    varies_on_cookie = [r for r in results if "cookie" in (r["headers_1"].get("vary", "").lower())]
    if not varies_on_cookie:
        reasons.append(
            "no response declared Vary: Cookie — the validator is very likely computed from the shared "
            "page, so it would not change when YOUR watched state does"
        )

    if reasons:
        return "NOT USABLE", reasons
    return "POSSIBLY USABLE", [
        "every check passed; still verify by hand that marking an episode watched changes the ETag "
        "before trusting it with real data"
    ]


async def main_async(count):
    urls = pick_urls(count)
    if not urls:
        print("No usable series URLs found in the index — nothing to probe.")
        return

    scraper = SCRAPER_CLS()
    print(f"→ Logging in and probing {len(urls)} page(s), read-only...\n")
    client = await scraper._acquire_client()
    try:
        results = []
        for url in urls:
            print(f"  · {url}")
            results.append(await probe_one(client, url))
    finally:
        await scraper._release_client()

    state, reasons = verdict(results)

    print("\n" + "=" * 70)
    print(f"  VERDICT: {state}")
    print("=" * 70)
    for reason in reasons:
        print(f"  - {reason}")

    print("\n  Per-page detail:")
    for r in results:
        h = r["headers_1"]
        print(f"\n  {r['url']}")
        print(f"    status           : {r['status_1']}")
        print(f"    etag             : {h.get('etag', '(none)')}")
        print(f"    last-modified    : {h.get('last-modified', '(none)')}")
        print(f"    cache-control    : {h.get('cache-control', '(none)')}")
        print(f"    vary             : {h.get('vary', '(none)')}")
        print(f"    cf-cache-status  : {h.get('cf-cache-status', '(none)')}")
        print(f"    etag stable      : {r['etag_stable']}")
        print(f"    body identical   : {r['body_identical']}")
        print(f"    conditional      : {r['conditional_sent'] or '(not sent)'}")
        print(f"    -> status        : {r['conditional_status']}  (304 = would skip)")

    payload = {
        "generated": datetime.now().isoformat(),
        "verdict": state,
        "reasons": reasons,
        "results": results,
    }
    REPORT_FILE.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\n  Saved: {REPORT_FILE}")
    print("  (this file contains only URLs and cache headers — no credentials)")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=5, help="how many pages to probe (default 5)")
    args = parser.parse_args()
    asyncio.run(main_async(max(1, args.count)))


if __name__ == "__main__":
    main()
