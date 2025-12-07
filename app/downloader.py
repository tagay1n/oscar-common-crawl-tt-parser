import os
import subprocess
import zlib
from pathlib import Path
from typing import Iterable

from rich import print
from rich.progress import track
from warcio.archiveiterator import ArchiveIterator

from app import db
from app.config import Settings


def write_path_file(settings: Settings, conn, path_file: Path) -> int:
    filenames = db.list_warc_filenames(conn)
    if not filenames:
        print("[yellow]No WARC filenames ready for download[/yellow]")
        return 0

    path_file.parent.mkdir(parents=True, exist_ok=True)
    with open(path_file, "w") as f:
        for name in sorted(set(filenames)):
            f.write(f"{name}\n")
    print(f"[green]Wrote {len(filenames)} paths -> {path_file}[/green]")
    return len(filenames)


def run_cc_downloader(settings: Settings, path_file: Path) -> None:
    if not path_file.exists():
        raise FileNotFoundError(f"Path file not found: {path_file}")
    if not settings.cc_binary.exists():
        raise FileNotFoundError(f"cc-downloader binary not found: {settings.cc_binary}")

    cmd = [
        str(settings.cc_binary),
        "download",
        "-t",
        str(settings.cc_threads),
        "-p",
        str(path_file),
        str(settings.warc_dir),
    ]
    print(f"[cyan]Running[/cyan]: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


def safe_filename(url: str, digest: str | None) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in "-._" else "_" for ch in url)
    cleaned = cleaned.strip("_")[:150]
    if digest:
        cleaned = f"{cleaned}_{digest}"
    return cleaned or (digest or "unknown")


def extract_html(settings: Settings, conn, limit: int | None = None) -> None:
    filenames = db.list_warc_filenames(conn)
    if limit:
        filenames = filenames[:limit]
    if not filenames:
        print("[yellow]No filenames with offsets to extract[/yellow]")
        return

    for filename in filenames:
        warc_path = settings.warc_dir / filename
        if not warc_path.exists():
            print(f"[red]Missing WARC file[/red] {warc_path}")
            continue

        rows = list(db.urls_for_warc(conn, filename))
        if not rows:
            continue

        print(f"[cyan]Extracting[/cyan] {filename} ({len(rows)} urls)")
        _extract_warc_file(settings, conn, warc_path, rows)


def _extract_warc_file(settings: Settings, conn, warc_path: Path, rows: Iterable) -> None:
    with open(warc_path, "rb") as fh:
        for row in track(rows, description=warc_path.name):
            try:
                fh.seek(int(row["offset"]))
                record = next(ArchiveIterator(fh))
                target = record.rec_headers.get_header("WARC-Target-URI")
                if target and row["url_raw"] and target.strip() != row["url_raw"].strip():
                    raise ValueError("WARC target mismatch")

                payload = record.content_stream().read()
                encoding = (
                    record.http_headers.get_header("Content-Encoding")
                    if record.http_headers
                    else None
                )
                if encoding and "gzip" in encoding.lower():
                    payload = zlib.decompress(payload, 16 + zlib.MAX_WBITS)

                text = payload.decode("utf-8", errors="replace")
                out_dir = settings.html_dir / row["snapshot_name"]
                out_dir.mkdir(parents=True, exist_ok=True)
                out_name = safe_filename(row["url_raw"], row["digest"]) + ".html"
                out_path = out_dir / out_name
                with open(out_path, "w", encoding="utf-8") as f:
                    f.write(text)

                db.mark_saved(conn, row["id"], str(out_path), status="downloaded")
            except Exception as e:
                db.record_error(conn, row["id"], str(e))
                print(f"[red]Failed {row['url_raw']}: {e}[/red]")
