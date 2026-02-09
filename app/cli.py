"""Typer CLI entrypoints for the OSCAR extraction workflow."""

from pathlib import Path

import typer
from rich import print

from app import cdx, db, downloader, export, hf, ingest
from app.config import load_settings

cli = typer.Typer(context_settings={"help_option_names": ["-h", "--help"]})


def _connect():
    """Load settings, open SQLite, and ensure schema availability."""
    settings = load_settings()
    conn = db.get_connection(settings.db_path)
    db.init_db(conn)
    return settings, conn


@cli.command("ingest")
def ingest_all(force: bool = typer.Option(False, help="Re-ingest already imported shards")):
    """Download and ingest all tt_meta shards into SQLite."""
    settings, conn = _connect()
    shards = hf.list_tt_meta_files(settings)
    for shard in shards:
        ingest.ingest_shard(settings, conn, shard, force=force)


@cli.command("resolve-offsets")
def resolve_offsets(limit: int = typer.Option(None, help="Limit number of URLs to resolve")):
    """Resolve offset/length/filename via CC CDX API."""
    settings, conn = _connect()
    cdx.resolve_missing(settings, conn, limit=limit)


@cli.command("resolve-offsets-local")
def resolve_offsets_local(
    limit: int = typer.Option(None, help="Limit number of URLs to resolve"),
    snapshot: str = typer.Option(None, help="Only process a specific snapshot name"),
):
    """Resolve offsets by downloading and scanning local CDX shards (no API rate limit)."""
    settings, conn = _connect()
    cdx.resolve_missing_local(settings, conn, snapshot=snapshot, limit=limit)


@cli.command("prepare-downloads")
def prepare_downloads(
    path_file: Path = typer.Option(None, help="Where to write the WARC path list")
):
    """Write a file listing WARC paths to feed cc-downloader."""
    settings, conn = _connect()
    path_file = path_file or settings.workdir / "warc_paths.txt"
    downloader.write_path_file(settings, conn, Path(path_file))


@cli.command("download-warcs")
def download_warcs(
    path_file: Path = typer.Option(None, help="Path file for cc-downloader")
):
    """Run cc-downloader to fetch WARC files."""
    settings, _ = _connect()
    path_file = path_file or settings.workdir / "warc_paths.txt"
    downloader.run_cc_downloader(settings, Path(path_file))


@cli.command("download-range")
def download_range(
    filename: str = typer.Argument(..., help="Common Crawl WARC path, e.g. crawl-data/CC-MAIN-2019-22/segments/.../warc.gz"),
    offset: int = typer.Argument(..., help="Byte offset in the WARC file (compressed)"),
    length: int = typer.Argument(..., help="Byte length to fetch"),
    dest: Path = typer.Option(None, help="Optional output path; defaults to workdir/warc/parts/<name>-<offset>-<end>.warc.gz"),
):
    """Download a single byte range from a Common Crawl WARC file."""
    settings, _ = _connect()
    downloader.download_range(settings, filename, offset, length, dest)


@cli.command("extract-html")
def extract_html(limit: int = typer.Option(None, help="Limit number of WARC files")):
    """Extract HTML responses from downloaded WARC files."""
    settings, conn = _connect()
    downloader.extract_html(settings, conn, limit=limit)


@cli.command("download-ranges")
def download_ranges(
    snapshot: str = typer.Option(None, help="Only process a specific snapshot name"),
    limit: int = typer.Option(None, help="Limit number of rows to fetch"),
):
    """Download HTML via HTTP Range (no full WARC download)."""
    settings, conn = _connect()
    downloader.download_missing_ranges(settings, conn, snapshot=snapshot, limit=limit)


@cli.command("stats")
def stats():
    """Quick progress overview."""
    settings, conn = _connect()
    cur = conn.execute(
        """
        SELECT
            COUNT(*) AS total,
            SUM(filename IS NOT NULL) AS resolved,
            SUM(saved_path IS NOT NULL) AS downloaded,
            SUM(status = 'error') AS errors
        FROM urls
        """
    )
    row = cur.fetchone()
    print(
        f"[green]URLs:[/green] {row['total']} total, "
        f"{row['resolved']} resolved, {row['downloaded']} downloaded, "
        f"{row['errors']} errors"
    )


@cli.command("export-parquet")
def export_parquet(
    snapshot: str = typer.Option(None, help="Only export a specific snapshot name"),
    limit: int = typer.Option(None, help="Limit number of rows to export"),
    split: float = typer.Option(
        None,
        help="Max parquet file size in megabytes; "
        "if omitted, write a single file per snapshot",
    ),
):
    """Export saved HTML to Parquet with markdown conversion."""
    settings, conn = _connect()
    export.export_parquet(
        settings,
        conn,
        snapshot=snapshot,
        limit=limit,
        split=split,
    )


if __name__ == "__main__":
    cli()
