"""Export saved HTML rows to Parquet with optional size splitting."""

from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq
import trafilatura
from rich import print
from rich.progress import track

from app import db
from app.config import Settings


def _read_html(path: Path) -> str | None:
    """Read UTF-8 HTML content from disk, returning None if missing."""
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except FileNotFoundError:
        return None


def _parse_split_mb(value: str | int | float) -> int:
    """Parse the split threshold in megabytes into bytes."""
    try:
        mb = float(value)
    except (TypeError, ValueError):
        raise ValueError("split must be a number of megabytes") from None
    if mb <= 0:
        raise ValueError("split must be positive")
    return int(mb * 1024 * 1024)


def _snapshot_basename(snapshot: str) -> str:
    """Convert `CC-MAIN-YYYY-NN` to `YYYY-NN` for file naming."""
    prefix = "CC-MAIN-"
    if snapshot.startswith(prefix):
        return snapshot[len(prefix) :]
    return snapshot


def export_parquet(
    settings: Settings,
    conn,
    snapshot: Optional[str] = None,
    limit: Optional[int] = None,
    split: Optional[str] = None,
) -> None:
    """Export saved HTML rows into parquet, optionally split by file size.

    The exporter iterates saved rows per snapshot, reads local HTML, generates
    markdown with trafilatura, and writes batched records through a Parquet
    writer. When `split` is provided, it rolls over to numbered part files once
    the current output file reaches the requested size.
    """
    max_bytes = _parse_split_mb(split) if split else None
    snapshots = [snapshot] if snapshot else db.snapshots_with_saved(conn)
    if not snapshots:
        print("[yellow]No saved HTML rows to export[/yellow]")
        return

    for snap in snapshots:
        rows_iter = db.iter_saved_rows(conn, snapshot=snap, limit=limit)
        row_count = conn.execute(
            """
            SELECT COUNT(*) AS total
            FROM urls
            JOIN snapshots ON snapshots.id = urls.snapshot_id
            WHERE urls.saved_path IS NOT NULL
              AND snapshots.snapshot_name = ?
            """,
            (snap,),
        ).fetchone()["total"]
        if limit is not None:
            row_count = min(row_count, limit)
        if row_count == 0:
            print(f"[yellow]Snapshot {snap}: nothing to export[/yellow]")
            continue

        print(f"[cyan]Exporting {row_count} rows from {snap}[/cyan]")
        records: list[dict] = []
        batch_size = 500
        writer: pq.ParquetWriter | None = None
        out_path: Path | None = None
        part_index = 0
        base_name = _snapshot_basename(snap)
        schema = pa.schema(
            [
                ("url", pa.string()),
                ("offset", pa.int64()),
                ("length", pa.int64()),
                ("filename", pa.string()),
                ("html", pa.string()),
                ("markdown", pa.string()),
            ]
        )

        def _open_writer(index: int) -> tuple[pq.ParquetWriter, Path]:
            """Create a parquet writer for the current part index."""
            if max_bytes:
                path = settings.parquet_dir / f"{base_name}_part{index:04d}.parquet"
            else:
                path = settings.parquet_dir / f"{base_name}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            return pq.ParquetWriter(path, schema), path

        def _flush() -> None:
            """Write buffered records and rotate files when split threshold is hit."""
            nonlocal writer, out_path, part_index, records
            if not records:
                return
            table = pa.Table.from_pylist(records, schema=schema)
            if writer is None:
                writer, out_path = _open_writer(part_index)
            writer.write_table(table)
            records = []
            if max_bytes and out_path and out_path.stat().st_size >= max_bytes:
                writer.close()
                writer = None
                part_index += 1

        for row in track(rows_iter, description=f"Building {snap}", total=row_count):
            html = _read_html(Path(row["saved_path"])) if row["saved_path"] else None
            markdown = (
                trafilatura.extract(html, output_format="markdown", with_metadata=True)
                if html
                else None
            )
            record = {
                "url": (row["url_raw"] or "").lower(),
                "offset": row["offset"],
                "length": row["length"],
                "filename": row["filename"],
                "html": html,
                "markdown": markdown,
            }
            records.append(record)
            if len(records) >= batch_size:
                _flush()

        _flush()
        if writer:
            writer.close()

        if max_bytes:
            parts_written = part_index + (1 if out_path else 0)
            if parts_written:
                print(f"[green]Wrote parquet parts[/green] {base_name}_part0000..{base_name}_part{parts_written - 1:04d}.parquet")
        elif out_path:
            print(f"[green]Wrote parquet[/green] {out_path}")
