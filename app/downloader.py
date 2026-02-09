"""Download WARC data and extract HTML from Common Crawl."""

import io
import random
import subprocess
import time
import zlib
from pathlib import Path
from typing import Iterable, Optional

import requests
from rich import print
from rich.progress import track
from warcio.archiveiterator import ArchiveIterator

from app import db
from app.config import Settings


def write_path_file(settings: Settings, conn, path_file: Path) -> int:
    """Write unique WARC filenames to a path list file for cc-downloader."""
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
    """Run the bundled cc-downloader binary against a prepared path list."""
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
    """Build a filesystem-safe name from URL and optional digest."""
    cleaned = "".join(ch if ch.isalnum() or ch in "-._" else "_" for ch in url)
    cleaned = cleaned.strip("_")[:150]
    if digest:
        cleaned = f"{cleaned}_{digest}"
    return cleaned or (digest or "unknown")


def download_range(
    settings: Settings,
    filename: str,
    offset: int,
    length: int,
    dest: Optional[Path] = None,
    chunk_size: int = 1024 * 1024,
) -> Path:
    """Download a byte range from a Common Crawl WARC file."""
    if length <= 0 or offset < 0:
        raise ValueError("offset must be >= 0 and length must be > 0")

    # Build URL and destination path.
    url = f"https://data.commoncrawl.org/{filename.lstrip('/')}"
    end = offset + length - 1
    dest = dest or (
        settings.warc_dir
        / "parts"
        / f"{Path(filename).name}-{offset}-{end}.warc.gz"
    )
    dest.parent.mkdir(parents=True, exist_ok=True)

    headers = {"Range": f"bytes={offset}-{end}"}
    tmp = dest.with_suffix(dest.suffix + ".part")

    print(f"[cyan]Downloading range {offset}-{end} from {filename}[/cyan]")
    with requests.get(url, headers=headers, stream=True, timeout=settings.cdx_timeout) as resp:
        resp.raise_for_status()
        with open(tmp, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=chunk_size):
                if chunk:
                    fh.write(chunk)
    tmp.replace(dest)
    print(f"[green]Saved[/green] {dest}")
    return dest


def extract_html(settings: Settings, conn, limit: int | None = None) -> None:
    """Extract HTML for resolved rows from downloaded WARC files.

    The function groups pending rows by WARC filename, verifies each archive is
    present locally, and delegates record-level extraction to `_extract_warc_file`.
    """
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
    """Read targeted records from a WARC file and persist HTML outputs.

    For each row, it seeks to the stored offset, parses one WARC record,
    validates the target URL, decodes/decompresses payload content when needed,
    writes HTML to snapshot-scoped output directories, and updates SQLite status.
    """
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


def download_missing_ranges(
    settings: Settings, conn, snapshot: str | None = None, limit: int | None = None
) -> None:
    """Fetch unresolved records directly via HTTP Range with retry/backoff logic.

    This path avoids downloading full WARC files: each row requests only its
    byte range, handles transient network/rate-limit/server failures, decodes the
    embedded response payload, and stores extracted HTML plus per-row status.
    """
    rows = list(db.iter_pending_html(conn, snapshot=snapshot, limit=limit))
    if not rows:
        print("[yellow]No pending rows with offsets to download[/yellow]")
        return

    print(f"[cyan]Downloading {len(rows)} ranges via HTTP Range requests[/cyan]")
    last_request = 0.0
    for row in track(rows, description="Fetching ranges"):
        try:
            start = int(row["offset"])
            length = int(row["length"])
            end = start + length - 1
            url = f"https://data.commoncrawl.org/{row['filename'].lstrip('/')}"
            headers = {
                "Range": f"bytes={start}-{end}",
                "User-Agent": "tt-html-extractor/1.0 (+https://huggingface.co/yasalma)",
            }

            body: bytes | None = None
            for attempt in range(settings.cdx_max_retries):
                # Polite pacing with jitter to avoid CC rate limits.
                now = time.time()
                target = settings.cdx_min_delay + random.uniform(0, 0.5)
                wait = target - (now - last_request)
                if wait > 0:
                    time.sleep(wait)
                last_request = time.time()

                try:
                    resp = requests.get(
                        url, headers=headers, stream=True, timeout=settings.cdx_timeout
                    )
                except requests.RequestException as e:
                    sleep_for = 2 * (attempt + 1)
                    print(f"[yellow]Network error {e}, sleeping {sleep_for}s[/yellow]")
                    time.sleep(sleep_for)
                    continue

                if resp.status_code in (429, 503):
                    retry_after = resp.headers.get("Retry-After")
                    if retry_after:
                        try:
                            sleep_for = float(retry_after)
                        except ValueError:
                            sleep_for = 3 * (attempt + 1) + random.uniform(0, 2)
                    else:
                        sleep_for = 3 * (attempt + 1) + random.uniform(0, 2)
                    print(f"[yellow]{resp.status_code} rate limit, sleeping {sleep_for:.1f}s[/yellow]")
                    time.sleep(sleep_for)
                    continue

                if resp.status_code >= 500:
                    sleep_for = 2 * (attempt + 1)
                    print(f"[yellow]Server {resp.status_code}, sleeping {sleep_for}s[/yellow]")
                    time.sleep(sleep_for)
                    continue

                if resp.status_code not in (200, 206):
                    raise requests.HTTPError(
                        f"Unexpected status {resp.status_code} for {url}"
                    )

                body = resp.content
                break

            if body is None:
                raise RuntimeError("Exhausted retries for range download")
            record = next(ArchiveIterator(io.BytesIO(body)))
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
