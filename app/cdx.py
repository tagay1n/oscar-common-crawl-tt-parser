"""Resolve Common Crawl offsets via CDX API calls or local shard scans."""

import gzip
import io
import json
import random
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import requests
from requests import Session
from rich import print
from rich.progress import track

from app import backoff, db
from app.config import Settings

CDX_NAME_RE = re.compile(r"cdx-\d{5}\.gz")
CDX_CHUNK_SIZE = 1024 * 1024
COMMIT_BATCH_SIZE = 200


@dataclass
class CDXMatch:
    """Resolved Common Crawl location for a single URL capture."""

    filename: str
    offset: int
    length: int
    timestamp: int


@dataclass
class RowState:
    """Mutable match state while scanning local CDX shards."""

    id: int
    warc_date: int | None
    filename: str | None
    offset: int | None
    length: int | None
    match: Optional[CDXMatch] = None


class RateLimiter:
    """Simple wall-clock throttler with optional random jitter."""

    def __init__(self, min_interval: float, jitter: float = 0.5):
        """Configure base delay and jitter applied before requests."""
        self.min_interval = min_interval
        self.jitter = jitter
        self.last = 0.0

    def wait(self) -> None:
        """Sleep until the next request is allowed."""
        now = time.time()
        # Add a small random jitter so we do not always hit CDX on a fixed cadence.
        target_interval = self.min_interval + random.uniform(0, self.jitter)
        wait = target_interval - (now - self.last)
        if wait > 0:
            time.sleep(wait)
        self.last = time.time()


def _parse_line(line: str) -> Optional[dict]:
    """Parse the JSON payload from one CDXJ line."""
    # CDXJ lines look like: "<urlkey> <timestamp> <json>"
    # We extract the JSON portion and parse that.
    try:
        brace = line.find("{")
        if brace == -1:
            return None
        obj = json.loads(line[brace:])
        return obj
    except Exception:
        return None


def _choose_best(items: Iterable[dict], warc_date: int | None) -> Optional[CDXMatch]:
    """Pick the best CDX record, preferring HTML and nearest timestamp."""
    filtered = []
    for it in items:
        if not (it.get("filename") and it.get("offset") and it.get("length")):
            continue
        mt = str(it.get("mimetype") or "").lower()
        if mt and "html" not in mt:
            continue
        filtered.append(it)

    if not filtered:
        return None

    if warc_date is None:
        best = filtered[0]
    else:
        filtered.sort(
            key=lambda it: abs(_ts(it.get("timestamp")) - warc_date) if it.get("timestamp") else 10**12
        )
        best = filtered[0]

    return CDXMatch(
        filename=best["filename"],
        offset=int(best["offset"]),
        length=int(best["length"]),
        timestamp=_ts(best.get("timestamp")),
    )


def _ts(value: str | None) -> int:
    """Parse a CDX timestamp string into epoch seconds."""
    if not value:
        return 0
    try:
        return int(time.mktime(time.strptime(value, "%Y%m%d%H%M%S")))
    except Exception:
        return 0


def _cdx_record_to_match(obj: dict) -> Optional[CDXMatch]:
    """Convert one parsed CDX object into `CDXMatch` when valid."""
    if not (obj.get("filename") and obj.get("offset") and obj.get("length")):
        return None

    mt = str(obj.get("mimetype") or obj.get("mime") or "").lower()
    if mt and "html" not in mt:
        return None

    return CDXMatch(
        filename=obj["filename"],
        offset=int(obj["offset"]),
        length=int(obj["length"]),
        timestamp=_ts(obj.get("timestamp")),
    )


def _better_match(current: Optional[CDXMatch], candidate: CDXMatch, warc_date: int | None) -> bool:
    """Return True if candidate should replace current for the target date."""
    if current is None:
        return True
    if warc_date:
        current_delta = abs(current.timestamp - warc_date) if current.timestamp else 10**12
        candidate_delta = abs(candidate.timestamp - warc_date) if candidate.timestamp else 10**12
        if candidate_delta < current_delta:
            return True
        if candidate_delta > current_delta:
            return False
    return False


def _list_snapshot_shards(
    settings: Settings, session: Session, snapshot_name: str, target_dir: Path
) -> tuple[list[tuple[str, Path]], list[tuple[str, Path]], list[Path]]:
    """Return (all_shards, missing_shards, all_dest_paths)."""
    index_paths_url = f"https://data.commoncrawl.org/crawl-data/{snapshot_name}/cc-index.paths.gz"
    all_entries: list[tuple[str, Path]] = []
    print(f"[cyan]Downloading shard listing from {index_paths_url}[/cyan]")
    try:
        resp = session.get(index_paths_url, timeout=settings.cdx_timeout)
        resp.raise_for_status()
        buf = io.BytesIO(resp.content)
        with gzip.GzipFile(fileobj=buf) as gzf:
            for raw_line in gzf:
                line = raw_line.decode("utf-8", errors="ignore").strip()
                if not line:
                    continue
                rel = line.split("commoncrawl/")[-1] if "commoncrawl/" in line else line
                basename = rel.rsplit("/", 1)[-1]
                if CDX_NAME_RE.fullmatch(basename):
                    url = f"https://data.commoncrawl.org/{rel}"
                    all_entries.append((url, target_dir / basename))
        all_entries = sorted(all_entries, key=lambda x: x[1].name)
        print(f"[cyan]Found {len(all_entries)} shard names from cc-index.paths.gz[/cyan]")
    except Exception as e:
        raise RuntimeError(f"Failed to list CDX shards for {snapshot_name}: {e}") from e

    if not all_entries:
        raise RuntimeError("No cdx-*.gz names found for snapshot")

    # Compare expected list with local files; re-download any missing, even if some shards exist.
    target_dir.mkdir(parents=True, exist_ok=True)
    all_paths: list[Path] = []
    missing: list[tuple[str, Path]] = []
    for url, dest in all_entries:
        all_paths.append(dest)
        if not dest.exists():
            missing.append((url, dest))

    return all_entries, missing, sorted(all_paths)


def _download_shard(settings: Settings, session: Session, url: str, dest: Path) -> None:
    """Download a single CDX shard to disk atomically."""
    tmp = dest.with_suffix(dest.suffix + ".part")
    with session.get(url, stream=True, timeout=settings.cdx_timeout) as resp:
        resp.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in resp.iter_content(chunk_size=CDX_CHUNK_SIZE):
                if chunk:
                    f.write(chunk)
    tmp.replace(dest)


def _ensure_snapshot_shards(
    settings: Settings, session: Session, snapshot_name: str
) -> list[Path]:
    """Ensure all shard files for a snapshot exist locally and return paths."""
    target_dir = settings.index_dir / snapshot_name
    print(f"[cyan]Gathering shard list for snapshot {snapshot_name}[/cyan]")
    shards, missing, all_paths = _list_snapshot_shards(
        settings, session, snapshot_name, target_dir
    )

    if missing:
        print(
            f"[cyan]Downloading {len(missing)} of {len(shards)} CDX shards to {target_dir}[/cyan]"
        )
        for url, dest in track(missing, description="Downloading CDX shards"):
            _download_shard(settings, session, url, dest)
    else:
        print(f"[cyan]All {len(shards)} CDX shards already present in {target_dir}[/cyan]")

    return sorted(all_paths)


def lookup_url(
    settings: Settings,
    session: Session,
    snapshot_name: str,
    url: str,
    warc_date: int | None,
    limiter: RateLimiter,
) -> Optional[CDXMatch]:
    """Resolve one URL via the online CDX API with retry/backoff behavior.

    Requests are rate-limited and retried on network errors, rate limits, and
    transient server failures. On success, the response lines are parsed and the
    best match is selected using MIME filtering and timestamp proximity.
    """
    base = f"https://index.commoncrawl.org/{snapshot_name}-index"
    params = {
        "url": url,
        "output": "json",
        "fl": "timestamp,length,offset,filename,mimetype,status,digest,original",
    }
    headers = {"User-Agent": "tt-html-extractor/1.0 (+https://huggingface.co/yasalma)"}

    for attempt in range(settings.cdx_max_retries):
        limiter.wait()
        try:
            resp = session.get(
                base, params=params, headers=headers, timeout=settings.cdx_timeout
            )
        except requests.RequestException as e:
            wait = backoff.linear_backoff(attempt)
            print(f"[yellow]Network error {e}, sleeping {wait}s[/yellow]")
            time.sleep(wait)
            continue

        if resp.status_code in (429, 503):
            wait = backoff.parse_retry_after_or_default(
                resp.headers.get("Retry-After"), attempt
            )
            print(f"[yellow]{resp.status_code} rate limit, sleeping {wait:.1f}s[/yellow]")
            time.sleep(wait)
            continue

        if resp.status_code >= 500:
            wait = backoff.linear_backoff(attempt)
            print(f"[yellow]Server {resp.status_code}, sleeping {wait}s[/yellow]")
            time.sleep(wait)
            continue

        if resp.status_code != 200:
            print(f"[red]CDX failed ({resp.status_code}) for {url}[/red]")
            return None

        lines = [line for line in resp.text.splitlines() if line.strip()]
        matches = [_parse_line(line) for line in lines]
        matches = [m for m in matches if m]
        return _choose_best(matches, warc_date)

    print(f"[red]CDX retries exhausted for {url}[/red]")
    return None


def resolve_missing(settings: Settings, conn, limit: int | None = None) -> None:
    """Resolve unresolved rows via CDX API queries and persist match results.

    For each row, the resolver tries multiple URL candidates (raw, lowercase,
    normalized), memoizes lookups per `(snapshot, url)` to reduce duplicate API
    requests, and writes either matched offsets or `missing` status back to SQLite.
    """
    cache: dict[tuple[str, str], Optional[CDXMatch]] = {}
    limiter = RateLimiter(settings.cdx_min_delay)

    rows = iter(db.iter_missing_offsets(conn, limit=limit))
    first_row = next(rows, None)
    if first_row is None:
        print("[green]No missing offsets to resolve[/green]")
        return
    if limit is not None:
        print(f"[cyan]Resolving offsets for up to {limit} urls[/cyan]")
    else:
        print("[cyan]Resolving offsets for pending urls[/cyan]")

    def row_iter():
        yield first_row
        yield from rows

    hits = 0
    misses = 0
    pending_writes = 0
    with requests.Session() as session:
        if limit is not None:
            loop = track(row_iter(), description="CDX lookups", total=limit)
        else:
            loop = row_iter()
        for row in loop:
            snapshot_name = row["snapshot_name"]
            candidates = []
            if row["url_raw"]:
                candidates.append(row["url_raw"])
                lower = row["url_raw"].lower()
                if lower != row["url_raw"]:
                    candidates.append(lower)
            if row["url_norm"] and row["url_norm"] not in candidates:
                candidates.append(row["url_norm"])

            match: Optional[CDXMatch] = None
            for cand in candidates:
                key = (snapshot_name, cand)
                if key in cache:
                    match = cache[key]
                else:
                    match = lookup_url(
                        settings, session, snapshot_name, cand, row["warc_date"], limiter
                    )
                    cache[key] = match
                if match:
                    break

            if match:
                hits += 1
                db.update_offset(
                    conn,
                    row["id"],
                    match.filename,
                    match.offset,
                    match.length,
                    status="matched",
                    commit=False,
                )
            else:
                misses += 1
                db.update_offset(
                    conn,
                    row["id"],
                    row["filename"],
                    row["offset"],
                    row["length"],
                    "missing",
                    commit=False,
                )
            pending_writes += 1
            if pending_writes >= COMMIT_BATCH_SIZE:
                conn.commit()
                pending_writes = 0
    if pending_writes:
        conn.commit()

    print(f"[green]CDX matched {hits} urls[/green], missing {misses}")


def _build_candidate_index(rows: list[dict]) -> tuple[list[RowState], dict[str, list[RowState]]]:
    """Build row states and reverse index from candidate URL to rows."""
    states: list[RowState] = []
    candidates: dict[str, list[RowState]] = {}
    for row in rows:
        state = RowState(
            id=row["id"],
            warc_date=row["warc_date"],
            filename=row["filename"],
            offset=row["offset"],
            length=row["length"],
        )
        states.append(state)

        urls: list[str] = []
        if row["url_raw"]:
            urls.append(row["url_raw"])
            lower = row["url_raw"].lower()
            if lower != row["url_raw"]:
                urls.append(lower)
        if row["url_norm"] and row["url_norm"] not in urls:
            urls.append(row["url_norm"])

        for url in urls:
            candidates.setdefault(url, []).append(state)

    return states, candidates


def _scan_shard(shard: Path, candidates: dict[str, list[RowState]]) -> None:
    """Scan one local CDX shard and update candidate row matches in place."""
    with gzip.open(shard, "rt", encoding="utf-8", errors="ignore") as f:
        for line in f:
            obj = _parse_line(line)
            if not obj:
                continue
            url = obj.get("url") or obj.get("original")
            if not url:
                continue
            targets = candidates.get(url)
            if not targets:
                continue
            match = _cdx_record_to_match(obj)
            if not match:
                continue
            for state in targets:
                if _better_match(state.match, match, state.warc_date):
                    state.match = match


def resolve_missing_local(
    settings: Settings, conn, snapshot: str | None = None, limit: int | None = None
) -> None:
    """Resolve missing offsets by scanning locally cached CDX shards.

    The resolver iterates target snapshots, ensures shard files are available,
    builds a candidate URL index for unresolved rows, scans every shard for
    matching CDX records, chooses best matches per row using timestamp proximity,
    and writes final matched/missing statuses into SQLite.
    """
    remaining = limit
    snapshots = [snapshot] if snapshot else db.snapshots_with_missing(conn)
    print(f"All snapshots: {snapshots}")
    if not snapshots:
        print("[green]No missing offsets to resolve[/green]")
        return

    total_matched = 0
    total_missing = 0

    with requests.Session() as session:
        for snap in snapshots:
            snap_limit = remaining if remaining is not None else None
            rows = list(db.iter_missing_offsets_for_snapshot(conn, snap, limit=snap_limit))
            if not rows:
                print(f"[yellow]Snapshot {snap}: nothing to resolve[/yellow]")
                continue

            print(
                f"[cyan]Snapshot {snap}: resolving {len(rows)} urls via local CDX shards[/cyan]"
            )

            try:
                shards = _ensure_snapshot_shards(settings, session, snap)
            except Exception as e:
                print(f"[red]{e}[/red]")
                if remaining is not None:
                    remaining -= len(rows)
                    if remaining <= 0:
                        break
                continue

            states, candidates = _build_candidate_index(rows)
            print(f"[cyan]Scanning {len(shards)} shards for {len(candidates)} candidate URLs[/cyan]")
            for shard in track(shards, description="Scanning CDX shards"):
                _scan_shard(shard, candidates)

            matched = 0
            missing = 0
            pending_writes = 0
            for state in track(states, description="Writing matches"):
                if state.match:
                    matched += 1
                    db.update_offset(
                        conn,
                        state.id,
                        state.match.filename,
                        state.match.offset,
                        state.match.length,
                        status="matched",
                        commit=False,
                    )
                else:
                    missing += 1
                    db.update_offset(
                        conn,
                        state.id,
                        state.filename,
                        state.offset,
                        state.length,
                        status="missing",
                        commit=False,
                    )
                pending_writes += 1
                if pending_writes >= COMMIT_BATCH_SIZE:
                    conn.commit()
                    pending_writes = 0
            if pending_writes:
                conn.commit()

            print(f"[green]{snap}: matched {matched}, missing {missing}[/green]")
            total_matched += matched
            total_missing += missing

            if remaining is not None:
                remaining -= len(rows)
                if remaining <= 0:
                    break

    print(
        f"[green]Done resolving locally: matched {total_matched}, missing {total_missing}[/green]"
    )
