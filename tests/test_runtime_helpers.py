"""Unit tests for deterministic helper logic in runtime modules."""

from datetime import datetime, timezone
import unittest

from app import cdx, downloader, hf, ingest


class SnapshotNameTests(unittest.TestCase):
    """Verify snapshot-name extraction from HF shard paths."""

    def test_extracts_cc_main_segment(self) -> None:
        path = "meta/tt/CC-MAIN-2019-22/tt_meta_0001.jsonl.zst"
        self.assertEqual(hf.snapshot_name_from_path(path), "CC-MAIN-2019-22")

    def test_promotes_year_week_segment(self) -> None:
        path = "meta/tt/2019-22/tt_meta_0001.jsonl.zst"
        self.assertEqual(hf.snapshot_name_from_path(path), "CC-MAIN-2019-22")

    def test_falls_back_to_path_stem(self) -> None:
        path = "meta/tt/custom_snapshot_file.jsonl.zst"
        self.assertEqual(hf.snapshot_name_from_path(path), "custom_snapshot_file.jsonl")


class IngestDateTests(unittest.TestCase):
    """Verify WARC date parsing behavior."""

    def test_parse_valid_warc_date(self) -> None:
        value = "2020-01-02T03:04:05Z"
        expected = int(datetime(2020, 1, 2, 3, 4, 5, tzinfo=timezone.utc).timestamp())
        self.assertEqual(ingest.parse_warc_date(value), expected)

    def test_parse_invalid_warc_date(self) -> None:
        self.assertIsNone(ingest.parse_warc_date("not-a-date"))

    def test_parse_missing_warc_date(self) -> None:
        self.assertIsNone(ingest.parse_warc_date(None))


class SafeFilenameTests(unittest.TestCase):
    """Verify filesystem-safe output naming."""

    def test_replaces_unsafe_chars(self) -> None:
        value = downloader.safe_filename("https://example.com/a b?c=d", None)
        self.assertEqual(value, "https___example.com_a_b_c_d")

    def test_appends_digest_suffix(self) -> None:
        value = downloader.safe_filename("https://example.com", "sha1:abc")
        self.assertTrue(value.endswith("_sha1:abc"))

    def test_truncates_long_names(self) -> None:
        url = "https://example.com/" + ("a" * 500)
        value = downloader.safe_filename(url, None)
        self.assertLessEqual(len(value), 150)


class CdxHelpersTests(unittest.TestCase):
    """Verify CDX parsing and best-match selection."""

    def test_parse_line_extracts_json_payload(self) -> None:
        line = (
            "com,example)/ 20200101000000 "
            '{"url":"https://example.com","filename":"a.warc.gz","offset":"10","length":"20"}'
        )
        parsed = cdx._parse_line(line)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["url"], "https://example.com")

    def test_parse_line_without_json_returns_none(self) -> None:
        self.assertIsNone(cdx._parse_line("not a cdxj line"))

    def test_cdx_record_to_match_filters_non_html(self) -> None:
        record = {
            "filename": "a.warc.gz",
            "offset": "10",
            "length": "20",
            "mimetype": "image/png",
            "timestamp": "20200101000000",
        }
        self.assertIsNone(cdx._cdx_record_to_match(record))

    def test_choose_best_prefers_closest_timestamp(self) -> None:
        items = [
            {
                "filename": "older.warc.gz",
                "offset": "10",
                "length": "20",
                "mimetype": "text/html",
                "timestamp": "20200101000000",
            },
            {
                "filename": "newer.warc.gz",
                "offset": "30",
                "length": "40",
                "mimetype": "text/html",
                "timestamp": "20200102120000",
            },
            {
                "filename": "ignored.warc.gz",
                "offset": "50",
                "length": "60",
                "mimetype": "image/png",
                "timestamp": "20200102115959",
            },
        ]
        warc_date = cdx._ts("20200102110000")
        best = cdx._choose_best(items, warc_date)
        self.assertIsNotNone(best)
        self.assertEqual(best.filename, "newer.warc.gz")


if __name__ == "__main__":
    unittest.main()
