"""Tests for configuration loading and CLI command wiring."""

from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import Mock, call, patch

import yaml
from typer.testing import CliRunner

from app import cli as cli_module
from app import config


class LoadSettingsTests(unittest.TestCase):
    """Verify token fallback and derived-path initialization."""

    def _write_config(self, root: Path, data: dict) -> Path:
        path = root / "config.yaml"
        path.write_text(yaml.safe_dump(data), encoding="utf-8")
        return path

    def test_uses_hf_token_from_environment_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = self._write_config(
                root,
                {
                    "hf": {"token": "", "repo": "oscar-corpus/community-oscar"},
                    "app": {"workdir": str(root / "work")},
                },
            )
            with patch.dict(os.environ, {"HF_TOKEN": "env-token"}, clear=False):
                settings = config.load_settings(str(cfg))

        self.assertEqual(settings.hf_token, "env-token")
        self.assertEqual(settings.hf_repo, "oscar-corpus/community-oscar")

    def test_raises_when_token_missing_everywhere(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = self._write_config(root, {"hf": {"token": ""}})
            with patch.dict(os.environ, {"HF_TOKEN": ""}, clear=False):
                with self.assertRaises(ValueError):
                    config.load_settings(str(cfg))

    def test_creates_default_subdirectories_under_configured_workdir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workdir = root / "custom-work"
            cfg = self._write_config(
                root,
                {
                    "hf": {"token": "inline-token"},
                    "app": {"workdir": str(workdir)},
                    "cc_downloader": {"threads": 4},
                    "cdx": {"min_delay": 0.2, "max_retries": 9, "timeout": 11},
                },
            )
            settings = config.load_settings(str(cfg))
            self.assertEqual(settings.workdir, workdir)
            self.assertEqual(settings.db_path, workdir / "state.sqlite")
            self.assertEqual(settings.cc_threads, 4)
            self.assertEqual(settings.cdx_min_delay, 0.2)
            self.assertEqual(settings.cdx_max_retries, 9)
            self.assertEqual(settings.cdx_timeout, 11)
            self.assertTrue(settings.shard_dir.exists())
            self.assertTrue(settings.index_dir.exists())
            self.assertTrue(settings.warc_dir.exists())
            self.assertTrue(settings.html_dir.exists())
            self.assertTrue(settings.parquet_dir.exists())


class CliWiringTests(unittest.TestCase):
    """Ensure CLI commands forward options to underlying handlers."""

    def setUp(self) -> None:
        self.runner = CliRunner()
        self.tmp = tempfile.TemporaryDirectory()
        self.settings = SimpleNamespace(workdir=Path(self.tmp.name))
        self.conn = Mock()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_ingest_forwards_force_and_each_shard(self) -> None:
        with patch("app.cli._connect", return_value=(self.settings, self.conn)):
            with patch("app.cli.hf.list_tt_meta_files", return_value=["a", "b"]):
                with patch("app.cli.ingest.ingest_shard") as ingest_shard:
                    result = self.runner.invoke(cli_module.cli, ["ingest", "--force"])
        self.assertEqual(result.exit_code, 0, result.stdout)
        ingest_shard.assert_has_calls(
            [
                call(self.settings, self.conn, "a", force=True),
                call(self.settings, self.conn, "b", force=True),
            ]
        )

    def test_resolve_commands_forward_options(self) -> None:
        with patch("app.cli._connect", return_value=(self.settings, self.conn)):
            with patch("app.cli.cdx.resolve_missing") as resolve_missing:
                result = self.runner.invoke(cli_module.cli, ["resolve-offsets", "--limit", "7"])
        self.assertEqual(result.exit_code, 0, result.stdout)
        resolve_missing.assert_called_once_with(self.settings, self.conn, limit=7)

        with patch("app.cli._connect", return_value=(self.settings, self.conn)):
            with patch("app.cli.cdx.resolve_missing_local") as resolve_missing_local:
                result = self.runner.invoke(
                    cli_module.cli,
                    ["resolve-offsets-local", "--limit", "5", "--snapshot", "CC-MAIN-2024-10"],
                )
        self.assertEqual(result.exit_code, 0, result.stdout)
        resolve_missing_local.assert_called_once_with(
            self.settings, self.conn, snapshot="CC-MAIN-2024-10", limit=5
        )

    def test_download_and_extract_commands_wire_paths_and_limits(self) -> None:
        with patch("app.cli._connect", return_value=(self.settings, self.conn)):
            with patch("app.cli.downloader.write_path_file") as write_path_file:
                result = self.runner.invoke(cli_module.cli, ["prepare-downloads"])
        self.assertEqual(result.exit_code, 0, result.stdout)
        write_path_file.assert_called_once_with(
            self.settings, self.conn, self.settings.workdir / "warc_paths.txt"
        )

        with patch("app.cli._connect", return_value=(self.settings, self.conn)):
            with patch("app.cli.downloader.run_cc_downloader") as run_cc_downloader:
                result = self.runner.invoke(cli_module.cli, ["download-warcs"])
        self.assertEqual(result.exit_code, 0, result.stdout)
        run_cc_downloader.assert_called_once_with(
            self.settings, self.settings.workdir / "warc_paths.txt"
        )

        with patch("app.cli._connect", return_value=(self.settings, self.conn)):
            with patch("app.cli.downloader.extract_html") as extract_html:
                result = self.runner.invoke(cli_module.cli, ["extract-html", "--limit", "3"])
        self.assertEqual(result.exit_code, 0, result.stdout)
        extract_html.assert_called_once_with(self.settings, self.conn, limit=3)

    def test_range_and_export_commands_forward_arguments(self) -> None:
        with patch("app.cli._connect", return_value=(self.settings, self.conn)):
            with patch("app.cli.downloader.download_range") as download_range:
                result = self.runner.invoke(
                    cli_module.cli,
                    [
                        "download-range",
                        "crawl-data/x.warc.gz",
                        "10",
                        "20",
                    ],
                )
        self.assertEqual(result.exit_code, 0, result.stdout)
        download_range.assert_called_once_with(
            self.settings,
            "crawl-data/x.warc.gz",
            10,
            20,
            None,
        )

        with patch("app.cli._connect", return_value=(self.settings, self.conn)):
            with patch("app.cli.downloader.download_missing_ranges") as download_missing_ranges:
                result = self.runner.invoke(
                    cli_module.cli,
                    ["download-ranges", "--snapshot", "CC-MAIN-2019-01", "--limit", "9"],
                )
        self.assertEqual(result.exit_code, 0, result.stdout)
        download_missing_ranges.assert_called_once_with(
            self.settings, self.conn, snapshot="CC-MAIN-2019-01", limit=9
        )

        with patch("app.cli._connect", return_value=(self.settings, self.conn)):
            with patch("app.cli.export.export_parquet") as export_parquet:
                result = self.runner.invoke(
                    cli_module.cli,
                    ["export-parquet", "--snapshot", "CC-MAIN-2020-10", "--limit", "11", "--split", "12.5"],
                )
        self.assertEqual(result.exit_code, 0, result.stdout)
        export_parquet.assert_called_once_with(
            self.settings,
            self.conn,
            snapshot="CC-MAIN-2020-10",
            limit=11,
            split=12.5,
        )

    def test_stats_command_runs_expected_query(self) -> None:
        cur = Mock()
        cur.fetchone.return_value = {"total": 1, "resolved": 1, "downloaded": 1, "errors": 0}
        self.conn.execute.return_value = cur

        with patch("app.cli._connect", return_value=(self.settings, self.conn)):
            result = self.runner.invoke(cli_module.cli, ["stats"])

        self.assertEqual(result.exit_code, 0, result.stdout)
        self.conn.execute.assert_called_once()


if __name__ == "__main__":
    unittest.main()
