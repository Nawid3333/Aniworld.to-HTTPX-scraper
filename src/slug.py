"""One canonical way to compare series slugs.

Two halves of this program learn a series' slug from different places. The
catalogue side reads the href the site prints; the index side reads whatever
was stored when the series was first scraped. Those are the same identifier,
but not always the same bytes: aniworld.to serves "/anime/stream/One%20Piece" in
one list and "/anime/stream/one%20piece" in another, and a percent escape
can arrive decoded on one side and encoded on the other.

Comparing them raw is how a series that is plainly on the site lands on the
"vanished" list. The index slug is missing from the site's slug set and the
site slug is missing from the index's, so one series is reported vanished and
new at the same time. Deleting it and re-scraping restores the exact same
pair, so the report never clears and the prompt returns every run.

Every comparison therefore goes through :func:`slug_key`. Extraction stays
where it is -- each site has its own URL shape, and the raw slug is still what
builds URLs -- but the moment a slug is used to answer "are these the same
series?", both sides go through here.

The normalisation is deliberately narrow: case and percent-encoding are two
spellings of one identifier, so they are folded. Separators are not: "a-b" and
"a b" can be two genuinely different series, and quietly merging them would
trade a false vanished report for a false duplicate.
"""

from __future__ import annotations

from urllib.parse import unquote

__all__ = ["slug_key", "slug_keys"]


def slug_key(value: str | None) -> str | None:
    """Return the comparison key for one slug, or None if there is nothing to compare.

    Decodes percent escapes once, collapses whitespace, and lowercases. Decoding
    once (rather than until stable) keeps the rule predictable: a slug that
    literally contains "%20" as text is left alone instead of being folded into
    the slug with a space.
    """
    if not isinstance(value, str):
        return None
    slug = unquote(value.strip()).strip("/")
    slug = " ".join(slug.split())
    return slug.lower() or None


def slug_keys(values) -> set[str]:
    """Return the comparison keys for an iterable of slugs, dropping empties.

    "unknown" is the sentinel the URL parsers return when a URL carries no
    slug at all; it is not an identifier and must never match another entry's
    missing slug, so it is dropped here rather than at each call site.
    """
    keys = {slug_key(v) for v in values}
    keys.discard(None)
    keys.discard("unknown")
    return keys  # type: ignore[return-value]
