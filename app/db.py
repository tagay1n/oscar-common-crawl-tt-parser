"""SQLite helpers for snapshot metadata and URL state."""

import sqlite3
from pathlib import Path
from typing import Iterable, Sequence


def get_connection(db_path: Path) -> sqlite3.Connection:
    """Open a SQLite connection configured for this workflow."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    """Create schema objects if they do not already exist."""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hf_path TEXT NOT NULL UNIQUE,
            snapshot_name TEXT NOT NULL,
            imported INTEGER NOT NULL DEFAULT 0,
            imported_at TEXT,
            total_urls INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS urls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_id INTEGER NOT NULL,
            url_raw TEXT NOT NULL,
            url_norm TEXT,
            warc_date INTEGER,
            warc_record_id TEXT,
            digest TEXT,
            offset INTEGER,
            length INTEGER,
            filename TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            saved_path TEXT,
            last_error TEXT,
            FOREIGN KEY(snapshot_id) REFERENCES snapshots(id)
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_urls_snapshot_url
            ON urls(snapshot_id, url_raw);
        CREATE INDEX IF NOT EXISTS idx_urls_filename
            ON urls(filename);
        CREATE INDEX IF NOT EXISTS idx_urls_status
            ON urls(status);
        CREATE INDEX IF NOT EXISTS idx_urls_saved_path
            ON urls(saved_path);
        CREATE INDEX IF NOT EXISTS idx_urls_snapshot_filename_offset
            ON urls(snapshot_id, filename, offset);
        CREATE INDEX IF NOT EXISTS idx_urls_unresolved_snapshot
            ON urls(snapshot_id)
            WHERE (offset IS NULL OR length IS NULL OR filename IS NULL);
        CREATE INDEX IF NOT EXISTS idx_urls_ready_extract
            ON urls(snapshot_id, filename, offset)
            WHERE filename IS NOT NULL
              AND offset IS NOT NULL
              AND length IS NOT NULL
              AND saved_path IS NULL;
        CREATE INDEX IF NOT EXISTS idx_snapshots_name
            ON snapshots(snapshot_name);
        """
    )
    conn.commit()


def ensure_snapshot(conn: sqlite3.Connection, hf_path: str, snapshot_name: str) -> int:
    """Insert or update a snapshot row and return its id."""
    cur = conn.execute(
        """
        INSERT INTO snapshots (hf_path, snapshot_name)
        VALUES (?, ?)
        ON CONFLICT(hf_path) DO UPDATE SET snapshot_name=excluded.snapshot_name
        """,
        (hf_path, snapshot_name),
    )
    if cur.lastrowid:
        return cur.lastrowid
    cur = conn.execute("SELECT id FROM snapshots WHERE hf_path = ?", (hf_path,))
    row = cur.fetchone()
    return int(row["id"])


def insert_urls(
    conn: sqlite3.Connection, snapshot_id: int, rows: Sequence[dict], batch_size: int = 500
) -> int:
    """Batch insert URL rows for one snapshot, ignoring duplicates."""
    inserted = 0
    for start in range(0, len(rows), batch_size):
        chunk = rows[start : start + batch_size]
        before = conn.total_changes
        conn.executemany(
            """
            INSERT OR IGNORE INTO urls
                (snapshot_id, url_raw, url_norm, warc_date, warc_record_id, digest)
            VALUES
                (:snapshot_id, :url_raw, :url_norm, :warc_date, :warc_record_id, :digest)
            """,
            [{**row, "snapshot_id": snapshot_id} for row in chunk],
        )
        inserted += conn.total_changes - before
    conn.commit()
    return inserted


def mark_snapshot_imported(
    conn: sqlite3.Connection, snapshot_id: int, total_urls: int, imported_at: str
) -> None:
    """Mark a snapshot as imported and store import stats."""
    conn.execute(
        """
        UPDATE snapshots
        SET imported = 1, imported_at = ?, total_urls = ?
        WHERE id = ?
        """,
        (imported_at, total_urls, snapshot_id),
    )
    conn.commit()


def iter_missing_offsets(conn: sqlite3.Connection, limit: int | None = None):
    """Iterate rows that still need filename/offset/length metadata."""
    sql = """
    SELECT
        urls.id,
        urls.url_raw,
        urls.url_norm,
        urls.warc_date,
        urls.digest,
        urls.warc_record_id,
        urls.filename,
        urls.offset,
        urls.length,
        snapshots.snapshot_name
    FROM urls
    JOIN snapshots ON snapshots.id = urls.snapshot_id
    WHERE (urls.offset IS NULL OR urls.length IS NULL OR urls.filename IS NULL)
    """
    params: list = []
    if limit:
        sql += " LIMIT ?"
        params.append(limit)
    return conn.execute(sql, params)


def iter_missing_offsets_for_snapshot(
    conn: sqlite3.Connection, snapshot_name: str, limit: int | None = None
):
    """Iterate unresolved rows for a single snapshot."""
    sql = """
    SELECT
        urls.id,
        urls.url_raw,
        urls.url_norm,
        urls.warc_date,
        urls.digest,
        urls.warc_record_id,
        urls.filename,
        urls.offset,
        urls.length,
        snapshots.snapshot_name
    FROM urls
    JOIN snapshots ON snapshots.id = urls.snapshot_id
    WHERE (urls.offset IS NULL OR urls.length IS NULL OR urls.filename IS NULL)
      AND snapshots.snapshot_name = ?
    """
    params: list = [snapshot_name]
    if limit:
        sql += " LIMIT ?"
        params.append(limit)
    return conn.execute(sql, params)


def snapshots_with_missing(conn: sqlite3.Connection) -> list[str]:
    """List snapshot names that still have unresolved offsets."""
    cur = conn.execute(
        """
        SELECT DISTINCT snapshots.snapshot_name
        FROM urls
        JOIN snapshots ON snapshots.id = urls.snapshot_id
        WHERE (urls.offset IS NULL OR urls.length IS NULL OR urls.filename IS NULL)
        ORDER BY snapshots.snapshot_name
        """
    )
    return [row["snapshot_name"] for row in cur.fetchall()]


def snapshots_with_saved(conn: sqlite3.Connection) -> list[str]:
    """List snapshot names with extracted HTML saved on disk."""
    cur = conn.execute(
        """
        SELECT DISTINCT snapshots.snapshot_name
        FROM urls
        JOIN snapshots ON snapshots.id = urls.snapshot_id
        WHERE urls.saved_path IS NOT NULL
        ORDER BY snapshots.snapshot_name
        """
    )
    return [row["snapshot_name"] for row in cur.fetchall()]


def iter_saved_rows(
    conn: sqlite3.Connection, snapshot: str | None = None, limit: int | None = None
):
    """Iterate rows that already have a saved HTML path."""
    sql = """
    SELECT urls.*, snapshots.snapshot_name
    FROM urls
    JOIN snapshots ON snapshots.id = urls.snapshot_id
    WHERE urls.saved_path IS NOT NULL
    """
    params: list = []
    if snapshot:
        sql += " AND snapshots.snapshot_name = ?"
        params.append(snapshot)
    sql += " ORDER BY urls.id"
    if limit:
        sql += " LIMIT ?"
        params.append(limit)
    return conn.execute(sql, params)


def update_offset(
    conn: sqlite3.Connection,
    url_id: int,
    filename: str | None,
    offset: int | None,
    length: int | None,
    status: str,
    commit: bool = True,
) -> None:
    """Persist resolved location metadata for one URL row."""
    conn.execute(
        """
        UPDATE urls
        SET filename = ?, offset = ?, length = ?, status = ?
        WHERE id = ?
        """,
        (filename, offset, length, status, url_id),
    )
    if commit:
        conn.commit()


def list_warc_filenames(conn: sqlite3.Connection) -> list[str]:
    """Return distinct WARC filenames that are ready for processing."""
    cur = conn.execute(
        """
        SELECT DISTINCT filename
        FROM urls
        WHERE filename IS NOT NULL
          AND offset IS NOT NULL
          AND length IS NOT NULL
          AND saved_path IS NULL
        """
    )
    return [row["filename"] for row in cur.fetchall()]


def iter_pending_html(
    conn: sqlite3.Connection, snapshot: str | None = None, limit: int | None = None
):
    """Iterate rows ready for HTML extraction but not yet saved."""
    sql = """
    SELECT urls.*, snapshots.snapshot_name
    FROM urls
    JOIN snapshots ON snapshots.id = urls.snapshot_id
    WHERE urls.filename IS NOT NULL
      AND urls.offset IS NOT NULL
      AND urls.length IS NOT NULL
      AND urls.saved_path IS NULL
    """
    params: list = []
    if snapshot:
        sql += " AND snapshots.snapshot_name = ?"
        params.append(snapshot)
    sql += " ORDER BY urls.filename, urls.offset"
    if limit:
        sql += " LIMIT ?"
        params.append(limit)
    return conn.execute(sql, params)


def urls_for_warc(conn: sqlite3.Connection, filename: str) -> Iterable[sqlite3.Row]:
    """Fetch pending rows associated with one WARC file."""
    cur = conn.execute(
        """
        SELECT urls.*, snapshots.snapshot_name
        FROM urls
        JOIN snapshots ON snapshots.id = urls.snapshot_id
        WHERE urls.filename = ?
          AND urls.offset IS NOT NULL
          AND urls.length IS NOT NULL
          AND urls.saved_path IS NULL
        """,
        (filename,),
    )
    return cur.fetchall()


def mark_saved(
    conn: sqlite3.Connection, url_id: int, path: str, status: str, commit: bool = True
) -> None:
    """Record the saved HTML path and clear prior error state."""
    conn.execute(
        """
        UPDATE urls
        SET saved_path = ?, status = ?, last_error = NULL
        WHERE id = ?
        """,
        (path, status, url_id),
    )
    if commit:
        conn.commit()


def record_error(
    conn: sqlite3.Connection, url_id: int, message: str, commit: bool = True
) -> None:
    """Store a truncated error message for a failed row."""
    conn.execute(
        """
        UPDATE urls
        SET last_error = ?, status = 'error'
        WHERE id = ?
        """,
        (message[:500], url_id),
    )
    if commit:
        conn.commit()
