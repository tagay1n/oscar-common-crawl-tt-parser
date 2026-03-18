"""Tests for CDX shard listing and local shard ensure logic."""

from __future__ import annotations

import gzip
from io import BytesIO
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from app import cdx
from app.config import Settings


def _make_settings(root: Path) -> Settings:
    """Build minimal settings rooted under a temporary directory."""
    return Settings(
        hf_token="test-token",
        hf_repo="repo",
        workdir=root,
        db_path=root / "state.sqlite",
        shard_dir=root / "shards",
        index_dir=root / "indexes",
        warc_dir=root / "warc",
        html_dir=root / "html",
        parquet_dir=root / "parquet",
        cc_binary=root / "cc-downloader",
        cc_threads=1,
        cdx_min_delay=0.0,
        cdx_max_retries=1,
        cdx_timeout=5,
    )


class _SimpleResponse:
    """Simple response with bytes payload and success status."""

    def __init__(self, content: bytes):
        self.content = content

    def raise_for_status(self) -> None:
        return None


class _SimpleSession:
    """Simple session returning one static listing response."""

    def __init__(self, content: bytes):
        self._content = content
        self.calls: list[tuple[str, int]] = []

    def get(self, url: str, timeout: int):
        self.calls.append((url, timeout))
        return _SimpleResponse(self._content)


class CdxShardListingTests(unittest.TestCase):
    """Validate parsing/filtering of snapshot shard manifests."""

    def test_list_snapshot_shards_filters_names_and_detects_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            settings = _make_settings(root)
            target = root / "indexes" / "CC-MAIN-2014-42"
            target.mkdir(parents=True, exist_ok=True)
            existing = target / "cdx-00002.gz"
            existing.write_bytes(b"already-present")

            lines = [
                "commoncrawl/crawl-data/CC-MAIN-2014-42/indexes/cdx-00002.gz",
                "crawl-data/CC-MAIN-2014-42/indexes/cdx-00001.gz",
                "crawl-data/CC-MAIN-2014-42/indexes/warc.paths.gz",
            ]
            buf = BytesIO()
            with gzip.GzipFile(fileobj=buf, mode="wb") as gzf:
                gzf.write(("\n".join(lines) + "\n").encode("utf-8"))
            session = _SimpleSession(buf.getvalue())

            all_shards, missing, all_paths = cdx._list_snapshot_shards(
                settings, session, "CC-MAIN-2014-42", target
            )

        self.assertEqual(len(all_shards), 2)
        self.assertEqual([p.name for _, p in all_shards], ["cdx-00001.gz", "cdx-00002.gz"])
        self.assertEqual(len(missing), 1)
        self.assertEqual(missing[0][1].name, "cdx-00001.gz")
        self.assertEqual([p.name for p in all_paths], ["cdx-00001.gz", "cdx-00002.gz"])
        self.assertEqual(len(session.calls), 1)
        self.assertIn("/cc-index.paths.gz", session.calls[0][0])

    def test_ensure_snapshot_shards_downloads_only_missing_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            settings = _make_settings(root)
            session = object()

            target = settings.index_dir / "CC-MAIN-2015-01"
            shard_a = target / "cdx-00001.gz"
            shard_b = target / "cdx-00002.gz"
            missing = [("https://data.commoncrawl.org/path/cdx-00001.gz", shard_a)]
            all_entries = [
                ("https://data.commoncrawl.org/path/cdx-00001.gz", shard_a),
                ("https://data.commoncrawl.org/path/cdx-00002.gz", shard_b),
            ]

            with patch(
                "app.cdx._list_snapshot_shards",
                return_value=(all_entries, missing, [shard_a, shard_b]),
            ):
                with patch("app.cdx._download_shard") as download_shard:
                    out = cdx._ensure_snapshot_shards(settings, session, "CC-MAIN-2015-01")

        download_shard.assert_called_once_with(
            settings,
            session,
            "https://data.commoncrawl.org/path/cdx-00001.gz",
            shard_a,
        )
        self.assertEqual(out, [shard_a, shard_b])


if __name__ == "__main__":
    unittest.main()
