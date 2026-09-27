"""The scraper only offers content encodings that httpx can decode here.

AniWorld's client used to send ``Accept-Encoding: gzip, deflate, br``
itself. httpx decodes br only when ``brotli`` or ``brotlicffi`` imports, and
neither is a dependency of this project, so on a machine without them a
server that chose br would have delivered raw compressed bytes as the page.
The site answers gzip today even when br is offered, so this was latent -- but
it was one server-side preference away from every page failing to parse.

Leaving the header to httpx fixes it for good: httpx builds the offer from the
decoders it actually has.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import httpx
from httpx import _decoders

import src.scraper as sc

REPO_ROOT = Path(__file__).resolve().parent.parent


def _offered(header: str) -> set[str]:
    return {part.split(";")[0].strip().lower() for part in header.split(",") if part.strip()}


async def _logged_in_client_header() -> str:
    scraper = sc.AniWorldScraper()
    with mock.patch.object(sc.AniWorldScraper, "_login_client", new=mock.AsyncMock()):
        client = await scraper._create_logged_in_client()
    try:
        return client.headers["accept-encoding"]
    finally:
        await client.aclose()


class TestAcceptEncoding(unittest.TestCase):
    def test_offers_only_what_httpx_can_decode(self):
        offered = _offered(asyncio.run(_logged_in_client_header()))
        self.assertTrue(offered, "some compression should still be offered")
        self.assertLessEqual(offered, set(_decoders.SUPPORTED_DECODERS))

    def test_the_offer_is_left_to_httpx(self):
        # Equal to what a bare httpx client sends, i.e. nothing overrides it.
        default = httpx.AsyncClient().headers["accept-encoding"]
        self.assertEqual(asyncio.run(_logged_in_client_header()), default)

    def test_without_brotli_br_is_not_offered(self):
        """Run in a fresh interpreter where brotli cannot be imported."""
        code = (
            "import sys, asyncio, json\n"
            "sys.modules['brotli'] = None\n"
            "sys.modules['brotlicffi'] = None\n"
            "from tests.test_accept_encoding import _logged_in_client_header\n"
            "print('<<RESULT>>' + json.dumps(asyncio.run(_logged_in_client_header())))\n"
        )
        env = dict(os.environ)
        with tempfile.TemporaryDirectory() as home:
            # A home of its own, so a real .env or data/ is never touched.
            env["ANIWORLD_HOME"] = home
            proc = subprocess.run(
                [sys.executable, "-c", code],
                cwd=REPO_ROOT,
                env=env,
                capture_output=True,
                text=True,
                timeout=120,
            )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        line = next(x for x in proc.stdout.splitlines() if x.startswith("<<RESULT>>"))
        offered = _offered(json.loads(line[len("<<RESULT>>") :]))
        self.assertNotIn("br", offered)
        self.assertIn("gzip", offered)


if __name__ == "__main__":
    unittest.main()
