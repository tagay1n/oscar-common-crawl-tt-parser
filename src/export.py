import json
import os
from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq
from rich import print
from rich.progress import track

from utils import load_snapshots


def export_snapshots_to_parquet(
    snapshot_file: str,
    output_dir: Optional[str] = None,
    include_body: bool = True,
) -> None:
    """Create one parquet file per snapshot with download metadata and content."""

    snapshots = load_snapshots(snapshot_file)
    if output_dir is None:
        output_dir = os.path.expanduser("~/.oscar/parquet")
    os.makedirs(output_dir, exist_ok=True)

    for filename, data in snapshots.items():
        related_path = data.get("related_file")
        if not related_path or not os.path.exists(related_path):
            print(f"[yellow]Skipping {filename}: related file missing[/yellow]")
            continue

        snapshot_id = data.get("snapshot_id")
        if not snapshot_id:
            try:
                snapshot_id = Path(filename).parts[1]
            except Exception:
                snapshot_id = "unknown"

        with open(related_path, "r") as f:
            related = json.load(f)

        rows = []
        for url, details in track(related.items(), description=f"Building rows for {snapshot_id}"):
            offset = details.get("offset")
            length = details.get("length")
            cc_filename = details.get("filename")
            if offset is None or length is None or not cc_filename:
                continue

            saved_path = details.get("saved_path")
            html = None
            if include_body and saved_path and os.path.exists(saved_path):
                try:
                    with open(saved_path, "r", encoding="utf-8", errors="replace") as fh:
                        html = fh.read()
                except Exception:
                    html = None

            rows.append(
                {
                    "snapshot_id": snapshot_id,
                    "snapshot_file": filename,
                    "url": url,
                    "warc_record_id": details.get("warc_record_id"),
                    "warc_date": details.get("warc_date"),
                    "digest": details.get("digest"),
                    "offset": int(offset),
                    "length": int(length),
                    "cc_filename": cc_filename,
                    "saved_path": saved_path,
                    "html": html,
                }
            )

        if not rows:
            print(f"[yellow]No rows to export for {snapshot_id}[/yellow]")
            continue

        table = pa.Table.from_pylist(rows)
        out_name = f"{snapshot_id}.parquet" if snapshot_id else "snapshot.parquet"
        out_path = os.path.join(output_dir, out_name)
        pq.write_table(table, out_path)
        print(f"[green]Wrote {len(rows)} rows to {out_path}[/green]")


