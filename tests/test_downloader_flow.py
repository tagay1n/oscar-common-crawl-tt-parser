"""Tests for downloader extraction and HTTP-range retry behavior."""

from __future__ import annotations

import gzip
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from app import db, downloader
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
        cdx_max_retries=3,
        cdx_timeout=5,
    )


class _FakeResponse:
    """Lightweight context-manager response for mocked requests calls."""

    def __init__(self, status_code: int, content: bytes = b"", headers: dict | None = None):
        self.status_code = status_code
        self.content = content
        self.headers = headers or {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeSession:
    """Mockable requests session with a pre-seeded response queue."""

    def __init__(self, responses: list[_FakeResponse | Exception]):
        self.responses = list(responses)
        self.calls: list[tuple[tuple, dict]] = []

    def get(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        nxt = self.responses.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return nxt

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _HeaderMap:
    """Minimal header map exposing `get_header`."""

    def __init__(self, values: dict[str, str]):
        self._values = values

    def get_header(self, name: str):
        return self._values.get(name)


class _FakeRecord:
    """Minimal WARC-like record returned by mocked ArchiveIterator."""

    def __init__(self, target_url: str, payload: bytes, content_encoding: str | None = None):
        self._payload = payload
        self.rec_headers = _HeaderMap({"WARC-Target-URI": target_url})
        http_vals: dict[str, str] = {}
        if content_encoding:
            http_vals["Content-Encoding"] = content_encoding
        self.http_headers = _HeaderMap(http_vals) if http_vals else None

    def content_stream(self):
        import io

        return io.BytesIO(self._payload)


class DownloaderFlowTests(unittest.TestCase):
    """Validate extraction and direct range-download control flow."""

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

    def _insert_row(self, snapshot: str, url: str, digest: str = "sha1:test") -> int:
        snapshot_id = db.ensure_snapshot(self.conn, f"{snapshot}/shard", snapshot)
        db.insert_urls(
            self.conn,
            snapshot_id,
            [
                {
                    "url_raw": url,
                    "url_norm": url,
                    "warc_date": None,
                    "warc_record_id": "rec",
                    "digest": digest,
                }
            ],
        )
        return int(
            self.conn.execute("SELECT id FROM urls WHERE url_raw = ?", (url,)).fetchone()["id"]
        )

    def test_extract_html_decodes_gzip_payload(self) -> None:
        url = "https://example.com/page"
        row_id = self._insert_row("CC-MAIN-2014-42", url)
        filename = "sample.warc"
        compressed = gzip.compress(b"<html>decoded</html>")

        warc_path = self.settings.warc_dir / filename
        warc_path.write_bytes(b"placeholder")
        db.update_offset(self.conn, row_id, filename, 0, len(b"placeholder"), "matched")

        fake_record = _FakeRecord(url, compressed, content_encoding="gzip")
        with patch("app.downloader.ArchiveIterator", return_value=iter([fake_record])):
            downloader.extract_html(self.settings, self.conn)

        row = self.conn.execute(
            "SELECT status, saved_path, last_error FROM urls WHERE id = ?", (row_id,)
        ).fetchone()
        self.assertEqual(row["status"], "downloaded")
        self.assertIsNone(row["last_error"])
        self.assertIsNotNone(row["saved_path"])
        self.assertIn("<html>decoded</html>", Path(row["saved_path"]).read_text(encoding="utf-8"))

    def test_extract_html_marks_error_on_target_mismatch(self) -> None:
        row_id = self._insert_row("CC-MAIN-2014-43", "https://example.com/expected")
        filename = "mismatch.warc"
        (self.settings.warc_dir / filename).write_bytes(b"placeholder")
        db.update_offset(self.conn, row_id, filename, 0, len(b"placeholder"), "matched")

        fake_record = _FakeRecord("https://example.com/actual", b"<html>x</html>")
        with patch("app.downloader.ArchiveIterator", return_value=iter([fake_record])):
            downloader.extract_html(self.settings, self.conn)

        row = self.conn.execute(
            "SELECT status, saved_path, last_error FROM urls WHERE id = ?", (row_id,)
        ).fetchone()
        self.assertEqual(row["status"], "error")
        self.assertIsNone(row["saved_path"])
        self.assertIn("WARC target mismatch", row["last_error"])

    def test_download_missing_ranges_retries_rate_limit_then_succeeds(self) -> None:
        url = "https://example.com/range-ok"
        row_id = self._insert_row("CC-MAIN-2014-44", url)
        db.update_offset(self.conn, row_id, "crawl-data/a.warc.gz", 0, 128, "matched")
        session = _FakeSession(
            [
                _FakeResponse(429, headers={"Retry-After": "0"}),
                _FakeResponse(206, content=b"ignored"),
            ]
        )

        with patch("app.downloader.requests.Session", return_value=session):
            with patch("app.downloader.time.sleep", return_value=None):
                with patch("app.downloader.random.uniform", return_value=0.0):
                    with patch(
                        "app.downloader.ArchiveIterator",
                        return_value=iter([_FakeRecord(url, b"<html>range-ok</html>")]),
                    ):
                        downloader.download_missing_ranges(self.settings, self.conn)

        self.assertEqual(len(session.calls), 2)
        row = self.conn.execute(
            "SELECT status, saved_path, last_error FROM urls WHERE id = ?", (row_id,)
        ).fetchone()
        self.assertEqual(row["status"], "downloaded")
        self.assertIsNotNone(row["saved_path"])
        self.assertIsNone(row["last_error"])

    def test_download_missing_ranges_marks_error_after_retry_exhaustion(self) -> None:
        url = "https://example.com/range-fail"
        row_id = self._insert_row("CC-MAIN-2014-45", url)
        db.update_offset(self.conn, row_id, "crawl-data/b.warc.gz", 0, 128, "matched")
        session = _FakeSession([_FakeResponse(503), _FakeResponse(503), _FakeResponse(503)])

        with patch("app.downloader.requests.Session", return_value=session):
            with patch("app.downloader.time.sleep", return_value=None):
                with patch("app.downloader.random.uniform", return_value=0.0):
                    downloader.download_missing_ranges(self.settings, self.conn)

        row = self.conn.execute(
            "SELECT status, saved_path, last_error FROM urls WHERE id = ?", (row_id,)
        ).fetchone()
        self.assertEqual(row["status"], "error")
        self.assertIsNone(row["saved_path"])
        self.assertIn("Exhausted retries for range download", row["last_error"])


if __name__ == "__main__":
    unittest.main()
