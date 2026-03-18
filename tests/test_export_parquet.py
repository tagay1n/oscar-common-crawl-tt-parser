"""Tests for parquet export splitting and output integrity."""

from __future__ import annotations

from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import pyarrow.parquet as pq

from app import db, export
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


class ExportParquetTests(unittest.TestCase):
    """Validate export output naming and split rollover behavior."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.settings = _make_settings(root)
        for path in (
            self.settings.shard_dir,
            self.settings.index_dir,
            self.settings.warc_dir,
            self.settings.html_dir,
            self.settings.parquet_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)

        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        db.init_db(self.conn)

    def tearDown(self) -> None:
        self.conn.close()
        self.tmp.cleanup()

    def _seed_saved_rows(self, snapshot: str, count: int) -> None:
        snapshot_id = db.ensure_snapshot(self.conn, f"{snapshot}/shard", snapshot)
        db.insert_urls(
            self.conn,
            snapshot_id,
            [
                {
                    "url_raw": f"https://example.com/{i}",
                    "url_norm": f"https://example.com/{i}",
                    "warc_date": None,
                    "warc_record_id": f"rec-{i}",
                    "digest": f"sha1:{i}",
                }
                for i in range(count)
            ],
        )

        html_path = self.settings.html_dir / "seed.html"
        html_path.write_text("<html>" + ("x" * 5000) + "</html>", encoding="utf-8")

        ids = [row["id"] for row in self.conn.execute("SELECT id FROM urls ORDER BY id").fetchall()]
        self.conn.executemany(
            """
            UPDATE urls
            SET filename = ?, offset = ?, length = ?, status = ?, saved_path = ?
            WHERE id = ?
            """,
            [
                ("crawl/seed.warc.gz", idx * 10, 10, "downloaded", str(html_path), row_id)
                for idx, row_id in enumerate(ids)
            ],
        )
        self.conn.commit()

    def test_export_split_rollover_writes_multiple_parts_with_full_row_count(self) -> None:
        snapshot = "CC-MAIN-2024-10"
        total_rows = 501
        self._seed_saved_rows(snapshot, total_rows)

        with patch("app.export.trafilatura.extract", return_value="md"):
            export.export_parquet(self.settings, self.conn, snapshot=snapshot, split=0.001)

        part_files = sorted(self.settings.parquet_dir.glob("2024-10_part*.parquet"))
        self.assertGreaterEqual(len(part_files), 2)

        written_rows = 0
        for path in part_files:
            written_rows += pq.read_table(path).num_rows
        self.assertEqual(written_rows, total_rows)

    def test_export_without_split_uses_single_snapshot_file_name(self) -> None:
        snapshot = "CC-MAIN-2025-01"
        self._seed_saved_rows(snapshot, 2)

        with patch("app.export.trafilatura.extract", return_value="md"):
            export.export_parquet(self.settings, self.conn, snapshot=snapshot, split=None)

        out_path = self.settings.parquet_dir / "2025-01.parquet"
        self.assertTrue(out_path.exists())
        self.assertEqual(pq.read_table(out_path).num_rows, 2)


if __name__ == "__main__":
    unittest.main()
