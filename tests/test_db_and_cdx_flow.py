"""Unit tests for database helpers and local CDX resolution flow."""

from __future__ import annotations

import gzip
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from app import cdx, db
from app.config import Settings


class DbHelpersTests(unittest.TestCase):
    """Exercise core SQLite helper behavior with an in-memory database."""

    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        db.init_db(self.conn)

    def tearDown(self) -> None:
        self.conn.close()

    def test_insert_deduplicates_and_missing_iteration_updates(self) -> None:
        snapshot_id = db.ensure_snapshot(self.conn, "path/shard-1", "CC-MAIN-2014-42")
        rows = [
            {
                "url_raw": "https://example.com/a",
                "url_norm": "https://example.com/a",
                "warc_date": 1,
                "warc_record_id": "rec-a",
                "digest": "sha1:a",
            },
            {
                "url_raw": "https://example.com/b",
                "url_norm": "https://example.com/b",
                "warc_date": 2,
                "warc_record_id": "rec-b",
                "digest": "sha1:b",
            },
            # Duplicate by (snapshot_id, url_raw), should be ignored.
            {
                "url_raw": "https://example.com/a",
                "url_norm": "https://example.com/a",
                "warc_date": 3,
                "warc_record_id": "rec-a2",
                "digest": "sha1:a2",
            },
        ]
        inserted = db.insert_urls(self.conn, snapshot_id, rows, batch_size=1)
        self.assertEqual(inserted, 2)

        unresolved = list(db.iter_missing_offsets(self.conn))
        self.assertEqual(len(unresolved), 2)

        row_a = self.conn.execute(
            "SELECT id FROM urls WHERE url_raw = ?", ("https://example.com/a",)
        ).fetchone()
        db.update_offset(self.conn, row_a["id"], "crawl/a.warc.gz", 10, 20, "matched")

        unresolved_after = list(db.iter_missing_offsets(self.conn))
        self.assertEqual(len(unresolved_after), 1)
        self.assertEqual(unresolved_after[0]["url_raw"], "https://example.com/b")

    def test_record_error_truncates_and_mark_saved_clears_error(self) -> None:
        snapshot_id = db.ensure_snapshot(self.conn, "path/shard-2", "CC-MAIN-2014-43")
        db.insert_urls(
            self.conn,
            snapshot_id,
            [
                {
                    "url_raw": "https://example.com/error",
                    "url_norm": "https://example.com/error",
                    "warc_date": 1,
                    "warc_record_id": "rec",
                    "digest": "sha1:e",
                }
            ],
        )
        row = self.conn.execute("SELECT id FROM urls").fetchone()

        db.record_error(self.conn, row["id"], "x" * 900)
        errored = self.conn.execute(
            "SELECT status, last_error FROM urls WHERE id = ?", (row["id"],)
        ).fetchone()
        self.assertEqual(errored["status"], "error")
        self.assertEqual(len(errored["last_error"]), 500)

        db.mark_saved(self.conn, row["id"], "/tmp/out.html", status="downloaded")
        saved = self.conn.execute(
            "SELECT status, saved_path, last_error FROM urls WHERE id = ?", (row["id"],)
        ).fetchone()
        self.assertEqual(saved["status"], "downloaded")
        self.assertEqual(saved["saved_path"], "/tmp/out.html")
        self.assertIsNone(saved["last_error"])

    def test_pending_html_and_urls_for_warc_filters_saved_rows(self) -> None:
        snapshot_id = db.ensure_snapshot(self.conn, "path/shard-3", "CC-MAIN-2014-44")
        db.insert_urls(
            self.conn,
            snapshot_id,
            [
                {
                    "url_raw": "https://example.com/one",
                    "url_norm": "https://example.com/one",
                    "warc_date": 1,
                    "warc_record_id": "rec-1",
                    "digest": "sha1:1",
                },
                {
                    "url_raw": "https://example.com/two",
                    "url_norm": "https://example.com/two",
                    "warc_date": 2,
                    "warc_record_id": "rec-2",
                    "digest": "sha1:2",
                },
            ],
        )

        rows = self.conn.execute("SELECT id, url_raw FROM urls ORDER BY id").fetchall()
        first_id = rows[0]["id"]
        second_id = rows[1]["id"]
        db.update_offset(self.conn, first_id, "crawl/shared.warc.gz", 100, 50, "matched")
        db.update_offset(self.conn, second_id, "crawl/shared.warc.gz", 200, 50, "matched")

        db.mark_saved(self.conn, second_id, "/tmp/saved-two.html", "downloaded")

        pending = list(db.iter_pending_html(self.conn))
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["id"], first_id)

        warc_rows = list(db.urls_for_warc(self.conn, "crawl/shared.warc.gz"))
        self.assertEqual(len(warc_rows), 1)
        self.assertEqual(warc_rows[0]["id"], first_id)

    def test_limit_zero_returns_no_rows_for_iterators(self) -> None:
        snapshot_id = db.ensure_snapshot(self.conn, "path/shard-4", "CC-MAIN-2014-46")
        db.insert_urls(
            self.conn,
            snapshot_id,
            [
                {
                    "url_raw": "https://example.com/zero",
                    "url_norm": "https://example.com/zero",
                    "warc_date": 1,
                    "warc_record_id": "rec-zero",
                    "digest": "sha1:zero",
                }
            ],
        )
        row_id = self.conn.execute("SELECT id FROM urls").fetchone()["id"]
        db.update_offset(self.conn, row_id, "crawl/z.warc.gz", 10, 20, "matched")

        self.assertEqual(len(list(db.iter_missing_offsets(self.conn, limit=0))), 0)
        self.assertEqual(
            len(
                list(
                    db.iter_missing_offsets_for_snapshot(
                        self.conn, "CC-MAIN-2014-46", limit=0
                    )
                )
            ),
            0,
        )
        self.assertEqual(len(list(db.iter_saved_rows(self.conn, limit=0))), 0)
        self.assertEqual(len(list(db.iter_pending_html(self.conn, limit=0))), 0)


class CdxShardScanTests(unittest.TestCase):
    """Validate shard scanning and in-memory candidate match updates."""

    def test_scan_shard_selects_best_html_match(self) -> None:
        rows = [
            {
                "id": 1,
                "url_raw": "https://example.com",
                "url_norm": None,
                "warc_date": cdx._ts("20200102110000"),
                "filename": None,
                "offset": None,
                "length": None,
            }
        ]
        states, candidates = cdx._build_candidate_index(rows)

        with tempfile.TemporaryDirectory() as tmp_dir:
            shard = Path(tmp_dir) / "cdx-00001.gz"
            with gzip.open(shard, "wt", encoding="utf-8") as fh:
                lines = [
                    {
                        "url": "https://example.com",
                        "filename": "older.warc.gz",
                        "offset": "10",
                        "length": "20",
                        "mimetype": "text/html",
                        "timestamp": "20200101000000",
                    },
                    {
                        "url": "https://example.com",
                        "filename": "ignored-image.warc.gz",
                        "offset": "30",
                        "length": "40",
                        "mimetype": "image/png",
                        "timestamp": "20200102115959",
                    },
                    {
                        "url": "https://example.com",
                        "filename": "newer.warc.gz",
                        "offset": "50",
                        "length": "60",
                        "mimetype": "text/html",
                        "timestamp": "20200102120000",
                    },
                ]
                for obj in lines:
                    fh.write(f"com,example)/ 0 {json.dumps(obj)}\n")

            cdx._scan_shard(shard, candidates)

        self.assertIsNotNone(states[0].match)
        self.assertEqual(states[0].match.filename, "newer.warc.gz")
        self.assertEqual(states[0].match.offset, 50)
        self.assertEqual(states[0].match.length, 60)


class ResolveMissingLocalTests(unittest.TestCase):
    """Verify snapshot routing and limit handling in local resolver."""

    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        db.init_db(self.conn)
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.settings = Settings(
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
        for path in (
            self.settings.shard_dir,
            self.settings.index_dir,
            self.settings.warc_dir,
            self.settings.html_dir,
            self.settings.parquet_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        self.conn.close()
        self.tmp.cleanup()

    def _insert_unresolved(self, snapshot: str, url: str, warc_date: int | None = None) -> int:
        snapshot_id = db.ensure_snapshot(self.conn, f"{snapshot}/shard.jsonl.zst", snapshot)
        db.insert_urls(
            self.conn,
            snapshot_id,
            [
                {
                    "url_raw": url,
                    "url_norm": url,
                    "warc_date": warc_date,
                    "warc_record_id": "rec",
                    "digest": "sha1:test",
                }
            ],
        )
        row = self.conn.execute(
            """
            SELECT urls.id
            FROM urls
            JOIN snapshots ON snapshots.id = urls.snapshot_id
            WHERE snapshots.snapshot_name = ? AND urls.url_raw = ?
            """,
            (snapshot, url),
        ).fetchone()
        return int(row["id"])

    def _write_shard(self, filename: str, url: str, matched_warc: str) -> Path:
        path = self.settings.index_dir / filename
        with gzip.open(path, "wt", encoding="utf-8") as fh:
            obj = {
                "url": url,
                "filename": matched_warc,
                "offset": "111",
                "length": "222",
                "mimetype": "text/html",
                "timestamp": "20200101000000",
            }
            fh.write(f"com,example)/ 0 {json.dumps(obj)}\n")
        return path

    def test_limit_stops_after_first_snapshot_batch(self) -> None:
        snap_a = "CC-MAIN-2014-42"
        snap_b = "CC-MAIN-2014-43"
        row_a = self._insert_unresolved(snap_a, "https://example.com/a")
        row_b = self._insert_unresolved(snap_b, "https://example.com/b")

        shard_a = self._write_shard("a-cdx.gz", "https://example.com/a", "crawl/a.warc.gz")
        shard_b = self._write_shard("b-cdx.gz", "https://example.com/b", "crawl/b.warc.gz")
        calls: list[str] = []

        def fake_ensure(_: Settings, __, snapshot_name: str) -> list[Path]:
            calls.append(snapshot_name)
            return {snap_a: [shard_a], snap_b: [shard_b]}[snapshot_name]

        with patch("app.cdx._ensure_snapshot_shards", side_effect=fake_ensure):
            cdx.resolve_missing_local(self.settings, self.conn, limit=1)

        self.assertEqual(calls, [snap_a])
        a = self.conn.execute(
            "SELECT status, filename, offset, length FROM urls WHERE id = ?", (row_a,)
        ).fetchone()
        b = self.conn.execute(
            "SELECT status, filename, offset, length FROM urls WHERE id = ?", (row_b,)
        ).fetchone()

        self.assertEqual(a["status"], "matched")
        self.assertEqual(a["filename"], "crawl/a.warc.gz")
        self.assertEqual(a["offset"], 111)
        self.assertEqual(a["length"], 222)

        self.assertEqual(b["status"], "pending")
        self.assertIsNone(b["filename"])
        self.assertIsNone(b["offset"])
        self.assertIsNone(b["length"])

    def test_snapshot_filter_processes_only_selected_snapshot(self) -> None:
        snap_a = "CC-MAIN-2014-50"
        snap_b = "CC-MAIN-2014-51"
        row_a = self._insert_unresolved(snap_a, "https://example.com/a")
        row_b = self._insert_unresolved(snap_b, "https://example.com/b")

        shard_a = self._write_shard("a-only-cdx.gz", "https://example.com/a", "crawl/a.warc.gz")
        shard_b = self._write_shard("b-only-cdx.gz", "https://example.com/b", "crawl/b.warc.gz")
        calls: list[str] = []

        def fake_ensure(_: Settings, __, snapshot_name: str) -> list[Path]:
            calls.append(snapshot_name)
            return {snap_a: [shard_a], snap_b: [shard_b]}[snapshot_name]

        with patch("app.cdx._ensure_snapshot_shards", side_effect=fake_ensure):
            cdx.resolve_missing_local(self.settings, self.conn, snapshot=snap_b)

        self.assertEqual(calls, [snap_b])
        a = self.conn.execute(
            "SELECT status, filename FROM urls WHERE id = ?", (row_a,)
        ).fetchone()
        b = self.conn.execute(
            "SELECT status, filename FROM urls WHERE id = ?", (row_b,)
        ).fetchone()

        self.assertEqual(a["status"], "pending")
        self.assertIsNone(a["filename"])
        self.assertEqual(b["status"], "matched")
        self.assertEqual(b["filename"], "crawl/b.warc.gz")


if __name__ == "__main__":
    unittest.main()
