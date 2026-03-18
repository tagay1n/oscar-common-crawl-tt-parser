"""Configuration loader and derived on-disk paths for the app."""

import os
from dataclasses import dataclass
from pathlib import Path

import yaml


DEFAULT_WORKDIR = Path(os.environ.get("OSCAR_APP_DIR", Path.home() / ".oscar")).expanduser()


@dataclass
class Settings:
    """Runtime configuration and derived filesystem locations."""

    hf_token: str
    hf_repo: str
    workdir: Path
    db_path: Path
    shard_dir: Path
    index_dir: Path
    warc_dir: Path
    html_dir: Path
    parquet_dir: Path
    cc_binary: Path
    cc_threads: int
    cdx_min_delay: float
    cdx_max_retries: int
    cdx_timeout: int


def load_settings(config_path: str = "config.yaml") -> Settings:
    """Load and validate runtime settings, then ensure required directories exist.

    Values come from `config.yaml` with environment fallback for secrets. The
    function also derives standard working paths under the configured app
    directory and creates them eagerly so downstream commands can assume the
    layout is present.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}

    hf_cfg = config.get("hf", {}) or {}
    # Prefer environment token so local secrets can stay outside tracked files.
    token = os.environ.get("HF_TOKEN") or hf_cfg.get("token")
    if not token:
        raise ValueError("Missing Hugging Face token (set hf.token or HF_TOKEN)")

    repo = hf_cfg.get("repo") or "oscar-corpus/community-oscar"

    app_cfg = config.get("app", {}) or {}
    workdir = Path(app_cfg.get("workdir", DEFAULT_WORKDIR)).expanduser()
    shard_dir = workdir / "shards"
    index_dir = workdir / "indexes"
    warc_dir = workdir / "warc"
    html_dir = workdir / "html"
    parquet_dir = workdir / "parquet"
    db_path = workdir / "state.sqlite"
    for path in (workdir, shard_dir, index_dir, warc_dir, html_dir, parquet_dir):
        path.mkdir(parents=True, exist_ok=True)

    cc_cfg = config.get("cc_downloader", {}) or {}
    cc_binary = Path(cc_cfg.get("binary", "cc-downloader/cc-downloader")).expanduser()
    cc_threads = int(cc_cfg.get("threads", 2))

    cdx_cfg = config.get("cdx", {}) or {}
    cdx_min_delay = float(cdx_cfg.get("min_delay", 1.5))
    cdx_max_retries = int(cdx_cfg.get("max_retries", 4))
    cdx_timeout = int(cdx_cfg.get("timeout", 30))

    return Settings(
        hf_token=token,
        hf_repo=repo,
        workdir=workdir,
        db_path=db_path,
        shard_dir=shard_dir,
        index_dir=index_dir,
        warc_dir=warc_dir,
        html_dir=html_dir,
        parquet_dir=parquet_dir,
        cc_binary=cc_binary,
        cc_threads=cc_threads,
        cdx_min_delay=cdx_min_delay,
        cdx_max_retries=cdx_max_retries,
        cdx_timeout=cdx_timeout,
    )
