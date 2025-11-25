import json
import os

import pyarrow as pa
import pyarrow.parquet as pq
from rich import print
from rich.progress import track
import trafilatura

from utils import load_snapshots


def export_snapshots_to_parquet(snapshot_file):
    """Create one parquet file per snapshot with download metadata and content."""

    snapshots = load_snapshots(snapshot_file)

    # Process snapshots one by one and stream rows to parquet to avoid OOM.
    for filename, data in list(snapshots.items())[6:7]:
        related_path = data.get("related_file")
        if not related_path or not os.path.exists(related_path):
            print(f"[yellow]Skipping {filename}: related file missing[/yellow]")
            continue

        if not (snapshot_id := data.get("snapshot_id")):
            raise ValueError("Snapshot ID not found for parquet export")
        with open(related_path, "r") as f:
            related = json.load(f)

        out_name = f"{snapshot_id}.parquet"
        output_dir = os.path.expanduser("~/.oscar/parquet")
        os.makedirs(output_dir, exist_ok=True)
        out_path = os.path.join(output_dir, out_name)

        writer = None
        batch = []
        batch_size = 50

        try:
            for url, details in track(related.items(), description=f"Writing rows for {snapshot_id}"):
                offset = details.get("offset")
                length = details.get("length")
                cc_filename = details.get("filename")
                if offset is None or length is None or not cc_filename:
                    # print(f"Missing offset/length/filename for url: {url}, details: {details}")
                    continue

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

                batch.append(
                    {
                        "url": url,
                        "warc_date": details.get("warc_date"),
                        "offset": int(offset),
                        "length": int(length),
                        "filename": cc_filename,
                        "html": html,
                        "markdown": markdown,
                    }
                )

                if len(batch) >= batch_size:
                    table = pa.Table.from_pylist(batch, schema=writer.schema if writer else None)
                    if writer is None:
                        writer = pq.ParquetWriter(out_path, table.schema)
                    writer.write_table(table)
                    batch.clear()

            if batch:
                table = pa.Table.from_pylist(batch, schema=writer.schema if writer else None)
                if writer is None:
                    writer = pq.ParquetWriter(out_path, table.schema)
                writer.write_table(table)

            if writer is None:
                print(f"[yellow]No rows to export for {snapshot_id}[/yellow]")
                continue

            print(f"[green]Wrote parquet to {out_path}[/green]")
        finally:
            if writer is not None:
                writer.close()
