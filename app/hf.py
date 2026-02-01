"""Hugging Face shard discovery and download helpers."""

from pathlib import Path
from typing import List

from huggingface_hub import hf_hub_download, list_repo_files
from rich import print

from app.config import Settings


def list_tt_meta_files(settings: Settings) -> List[str]:
    print("[cyan]Listing tt_meta shards from Hugging Face...[/cyan]")
    files = list_repo_files(
        settings.hf_repo, repo_type="dataset", token=settings.hf_token
    )
    selected = sorted(
        f for f in files if "tt_meta" in f and f.endswith(".jsonl.zst")
    )
    print(f"[green]Found {len(selected)} shards[/green]")
    return selected


def download_shard(settings: Settings, filename: str) -> Path:
    """Download one shard to the local cache."""
    path = hf_hub_download(
        repo_id=settings.hf_repo,
        filename=filename,
        repo_type="dataset",
        token=settings.hf_token,
        local_dir=settings.shard_dir,
        local_dir_use_symlinks=False,
    )
    return Path(path)


def snapshot_name_from_path(hf_path: str) -> str:
    """Best-effort snapshot name extraction."""
    parts = Path(hf_path).parts
    for part in parts:
        if part.startswith("CC-MAIN-"):
            return part
        if part.replace("-", "").isdigit() and len(part.split("-")) == 2:
            return f"CC-MAIN-{part}"
    return Path(hf_path).stem
