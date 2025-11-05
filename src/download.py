import json
from utils import load_snapshots
import requests
from rich import print
from rich.progress import track
import os
from urllib.parse import urlparse
from pathlib import Path
# from warcio.archiveiterator import ArchiveIterator


def download(snapshot_file):
    snapshots = load_snapshots(snapshot_file)
    
    for filename, data in snapshots.items():
        if not data.get("offsets_extracted", False):
            continue
        print(f"Processing snapshot {filename}")
        try:
            _process_snapshot(data)
        except Exception as e:
            print(f"Error processing snapshot {filename}: {e}")


def _process_snapshot(data):
    related_file_path = data.get("related_file", None)
    if not related_file_path:
        return
    snapshot_id = data.get("snapshot_id")
    out_root = os.path.expanduser("~/.oscar/html")
    out_dir = os.path.join(out_root, snapshot_id)
    os.makedirs(out_dir, exist_ok=True)
    
    related = _related_file(related_file_path)
    for url, details in track(related.items(), description="Downloading URLs"):
        # Skip if already downloaded
        if details.get("saved_path", None):
            continue
        # Must have filename/offset/length from Common Crawl index
        filename = details.get("filename")
        offset = details.get("offset")
        length = details.get("length")
        if not filename or offset is None or length is None:
            raise ValueError(f"Missing filename/offset/length for url: {url}, details: {details}")
        # Prepare request
        start = int(offset)
        end = start + int(length) - 1
        headers = {"Range": f"bytes={start}-{end}"}
        cc_url = f"https://data.commoncrawl.org/{filename}"
        try:
            r = requests.get(cc_url, headers=headers, stream=True, timeout=60)
            r.raise_for_status()
            # Build deterministic file name
            out_name = _safe_filename(url, details.get("digest"))
            out_path = os.path.join(out_dir, out_name + ".html")
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 64):
                    if chunk:
                        f.write(chunk)
            # details["saved_path"] = out_path
            # Persist progress after each success
            _dump_related_file(related_file_path, related)
        except Exception as e:
            # Best-effort; continue to next
            print(f"Error downloading {url}: {e}")
            continue

def _related_file(related_file):
    with open(related_file, "r") as f:
        return json.load(f)
    

def _dump_related_file(related_file_path, data):
    part = f"{related_file_path}.part"
    with open(part, "w") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    os.rename(part, related_file_path)


def _safe_filename(url: str, digest: str | None) -> str:
    try:
        parsed = urlparse(url)
        base = (parsed.netloc + parsed.path).replace("/", "_")
        base = base.strip("_")
    except Exception:
        base = "unknown"
    if digest:
        base = f"{base[:150]}_{digest}"
    return base[:200] or (digest or "unknown")
