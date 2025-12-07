import gzip
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

from app import db
from app.config import Settings

CDX_NAME_RE = re.compile(r"cdx-\d{5}\.gz")
CDX_CHUNK_SIZE = 1024 * 1024


@dataclass
class CDXMatch:
    filename: str
    offset: int
    length: int
    timestamp: int


@dataclass
class RowState:
    id: int
    warc_date: int | None
    filename: str | None
    offset: int | None
    length: int | None
    match: Optional[CDXMatch] = None


class RateLimiter:
    def __init__(self, min_interval: float, jitter: float = 0.5):
        self.min_interval = min_interval
        self.jitter = jitter
        self.last = 0.0

    def wait(self) -> None:
        now = time.time()
        # Add a small random jitter so we do not always hit CDX on a fixed cadence.
        target_interval = self.min_interval + random.uniform(0, self.jitter)
        wait = target_interval - (now - self.last)
        if wait > 0:
            time.sleep(wait)
        self.last = time.time()


def _parse_line(line: str) -> Optional[dict]:
    try:
        obj = json.loads(line)
        return obj
    except Exception:
        return None


def _choose_best(items: Iterable[dict], warc_date: int | None) -> Optional[CDXMatch]:
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
    if not value:
        return 0
    try:
        return int(time.mktime(time.strptime(value, "%Y%m%d%H%M%S")))
    except Exception:
        return 0


def _cdx_record_to_match(obj: dict) -> Optional[CDXMatch]:
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
) -> list[Path]:
    # Prefer local files if already present.
    local = sorted(target_dir.glob("cdx-*.gz"))
    if local:
        return local

    # Try S3-style prefix listing first.
    prefix_url = f"https://data.commoncrawl.org/crawl-data/{snapshot_name}/?prefix=indexes/"
    names: list[str] = []
    try:
        resp = session.get(prefix_url, timeout=settings.cdx_timeout)
        if resp.status_code == 200:
            names = sorted(set(CDX_NAME_RE.findall(resp.text)))
    except Exception:
        pass

    # Fallback: probe sequential shard names with HEAD until a run of misses.
    if not names:
        base = f"https://data.commoncrawl.org/crawl-data/{snapshot_name}/indexes"
        hits = []
        consecutive_misses = 0
        max_misses = 5
        max_probes = 5000
        for i in range(max_probes):
            shard_name = f"cdx-{i:05}.gz"
            url = f"{base}/{shard_name}"
            try:
                r = session.head(url, timeout=settings.cdx_timeout, allow_redirects=True)
            except Exception:
                consecutive_misses += 1
                if consecutive_misses >= max_misses:
                    break
                continue

            if r.status_code == 200:
                hits.append(shard_name)
                consecutive_misses = 0
            else:
                consecutive_misses += 1
                if hits and consecutive_misses >= max_misses:
                    break

        names = sorted(set(hits))

    if not names:
        raise RuntimeError("No cdx-*.gz names found for snapshot")

    target_dir.mkdir(parents=True, exist_ok=True)
    return [target_dir / name for name in names]


def _download_shard(settings: Settings, session: Session, url: str, dest: Path) -> None:
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
    target_dir = settings.index_dir / snapshot_name
    shards = _list_snapshot_shards(settings, session, snapshot_name, target_dir)
    base = f"https://data.commoncrawl.org/crawl-data/{snapshot_name}/indexes"

    for shard in shards:
        if shard.exists():
            continue
        url = f"{base}/{shard.name}"
        print(f"[cyan]Downloading CDX shard {shard.name}[/cyan]")
        _download_shard(settings, session, url, shard)
    return sorted(shards)


def lookup_url(
    settings: Settings,
    session: Session,
    snapshot_name: str,
    url: str,
    warc_date: int | None,
    limiter: RateLimiter,
) -> Optional[CDXMatch]:
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
            wait = 2 * (attempt + 1)
            print(f"[yellow]Network error {e}, sleeping {wait}s[/yellow]")
            time.sleep(wait)
            continue

        if resp.status_code in (429, 503):
            retry_after = resp.headers.get("Retry-After")
            if retry_after:
                try:
                    wait = float(retry_after)
                except ValueError:
                    wait = 3 * (attempt + 1) + random.uniform(0, 2)
            else:
                wait = 3 * (attempt + 1) + random.uniform(0, 2)
            print(f"[yellow]{resp.status_code} rate limit, sleeping {wait:.1f}s[/yellow]")
            time.sleep(wait)
            continue

        if resp.status_code >= 500:
            wait = 2 * (attempt + 1)
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
    cache: dict[tuple[str, str], Optional[CDXMatch]] = {}
    limiter = RateLimiter(settings.cdx_min_delay)

    rows = list(db.iter_missing_offsets(conn, limit=limit))
    print(f"[cyan]Resolving offsets for {len(rows)} urls[/cyan]")

    hits = 0
    misses = 0
    with requests.Session() as session:
        for row in track(rows, description="CDX lookups"):
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
                )
            else:
                misses += 1
                db.update_offset(conn, row["id"], row["filename"], row["offset"], row["length"], "missing")

    print(f"[green]CDX matched {hits} urls[/green], missing {misses}")


def _build_candidate_index(rows: list[dict]) -> tuple[list[RowState], dict[str, list[RowState]]]:
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
    remaining = limit
    snapshots = [snapshot] if snapshot else db.snapshots_with_missing(conn)
    if not snapshots:
        print("[green]No missing offsets to resolve[/green]")
        return

    with requests.Session() as session:
        for snap in snapshots:
            snap_limit = remaining if remaining is not None else None
            rows = list(db.iter_missing_offsets_for_snapshot(conn, snap, limit=snap_limit))
            if not rows:
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
            for shard in shards:
                _scan_shard(shard, candidates)

            matched = 0
            missing = 0
            for state in states:
                if state.match:
                    matched += 1
                    db.update_offset(
                        conn,
                        state.id,
                        state.match.filename,
                        state.match.offset,
                        state.match.length,
                        status="matched",
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
                    )

            print(f"[green]{snap}: matched {matched}, missing {missing}[/green]")

            if remaining is not None:
                remaining -= len(rows)
                if remaining <= 0:
                    break
