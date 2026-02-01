"""Ingest OSCAR TT shards into SQLite with URL normalization."""

import io
import json
from datetime import datetime, timezone
from typing import Dict

import urlcanon
import zstandard as zstd
from rich import print
from rich.progress import track

from app import db
from app.config import Settings
from app.hf import download_shard, snapshot_name_from_path


def parse_warc_date(value: str | None) -> int | None:
    if not value:
        return None
    try:
        dt = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
        return int(dt.replace(tzinfo=timezone.utc).timestamp())
    except Exception:
        return None


def ingest_shard(settings: Settings, conn, hf_path: str, force: bool = False) -> None:
    snapshot_name = snapshot_name_from_path(hf_path)
    snapshot_id = db.ensure_snapshot(conn, hf_path, snapshot_name)

    cur = conn.execute(
        "SELECT imported FROM snapshots WHERE id = ?", (snapshot_id,)
    )
    row = cur.fetchone()
    if row and row["imported"] and not force:
        print(f"[yellow]Skipping already imported shard[/yellow] {hf_path}")
        return

    shard_path = download_shard(settings, hf_path)
    print(f"[cyan]Ingesting[/cyan] {hf_path} -> {shard_path}")

    rows: list[Dict] = []
    total = 0
    inserted_total = 0

    with open(shard_path, "rb") as fh:
        dctx = zstd.ZstdDecompressor(max_window_size=2**31)
        with dctx.stream_reader(fh) as reader:
            text_stream = io.TextIOWrapper(reader, encoding="utf-8")
            for line in track(text_stream, description="Scanning records"):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue

                warc_headers = record.get("warc_headers", {}) or {}
                url_raw = warc_headers.get("warc-target-uri")
                if not url_raw:
                    continue

                url_norm = str(urlcanon.parse_url(url_raw)) if url_raw else None
                warc_date = parse_warc_date(warc_headers.get("warc-date"))
                row = {
                    "url_raw": url_raw,
                    "url_norm": url_norm,
                    "warc_date": warc_date,
                    "warc_record_id": warc_headers.get("warc-record-id"),
                    "digest": warc_headers.get("warc-block-digest")
                    or warc_headers.get("warc-payload-digest"),
                }
                rows.append(row)
                total += 1

                if len(rows) >= 1000:
                    inserted_total += db.insert_urls(conn, snapshot_id, rows)
                    rows.clear()
                    print(f"  inserted so far: {inserted_total} / {total}")

    if rows:
        inserted_total += db.insert_urls(conn, snapshot_id, rows)

    db.mark_snapshot_imported(conn, snapshot_id, total, datetime.utcnow().isoformat())
    print(
        f"[green]Finished[/green] {hf_path} ({total} records, {inserted_total} new)"
    )
