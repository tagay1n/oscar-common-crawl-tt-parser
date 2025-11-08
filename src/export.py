import json
import os
from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq
from rich import print
from rich.progress import track
import trafilatura

from utils import load_snapshots


def export_snapshots_to_parquet(snapshot_file):
    """Create one parquet file per snapshot with download metadata and content."""

    snapshots = load_snapshots(snapshot_file)

    for filename, data in list(snapshots.items())[:1]:
        related_path = data.get("related_file")
        if not related_path or not os.path.exists(related_path):
            print(f"[yellow]Skipping {filename}: related file missing[/yellow]")
            continue

        if not (snapshot_id := data.get("snapshot_id")):
            raise ValueError("Snapshot ID not found for parquet export")
        with open(related_path, "r") as f:
            related = json.load(f)

        rows = []
        for url, details in track(related.items(), description=f"Building rows for {snapshot_id}"):
            offset = details.get("offset")
            length = details.get("length")
            cc_filename = details.get("filename")
            if offset is None or length is None or not cc_filename:
                raise ValueError(f"Missing offset/length/filename for url: {url}, details: {details}")

            saved_path = details.get("saved_path")
            if not saved_path or not os.path.exists(saved_path):
                raise ValueError(f"Missing saved path for url: {url}, details: {details}")

            try:
                with open(saved_path, "r", encoding="utf-8", errors="replace") as fh:
                    html = fh.read()
            except Exception:
                raise ValueError(f"[yellow]Failed to read HTML from {saved_path}[/yellow]")

            # Convert HTML to markdown using trafilatura
            try:
                markdown = trafilatura.extract(
                    html,
                    output_format="markdown",
                    include_formatting=True,
                    include_tables=True,
                    include_images=True,
                    include_links=True,
                    include_comments=True,
                )
            except Exception as e:
                raise ValueError(f"[yellow]Failed to convert HTML to markdown for {url}: {e}[/yellow]")

            rows.append(
                {
                    "url": url,
                    "offset": int(offset),
                    "length": int(length),
                    "filename": cc_filename,
                    "html": html,
                    "markdown": markdown,
                }
            )

        if not rows:
            print(f"[yellow]No rows to export for {snapshot_id}[/yellow]")
            continue

        table = pa.Table.from_pylist(rows)
        out_name = f"{snapshot_id}.parquet"
        output_dir = os.path.expanduser("~/.oscar/parquet")
        os.makedirs(output_dir, exist_ok=True)
        out_path = os.path.join(output_dir, out_name)
        pq.write_table(table, out_path)
        print(f"[green]Wrote {len(rows)} rows to {out_path}[/green]")


