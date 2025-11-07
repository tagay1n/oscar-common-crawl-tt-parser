import json
from rich.progress import track
from utils import read_config, load_snapshots, dump_snapshots
from huggingface_hub import hf_hub_download
import os
import zstandard as zstd
import io
from pathlib import Path
import requests
import gzip
from rich import print
from datetime import timezone, datetime
from collections import defaultdict
import re


def collect_uris(snapshots_path, related_files_dir):
    config = read_config()
    records = defaultdict(dict)
    
    snapshots = load_snapshots(snapshots_path)
    oscars_ds_path = None
    
    for filename, data in snapshots.items():
        print(f"Processing file: '{filename}'")
        
        if data.get("related_file", None):
            continue
        try:
            print(f"Extracting uri from file: '{filename}'")
            oscars_ds_path = hf_hub_download(repo_id=config['hf']['repo'], filename=filename, repo_type="dataset")
            related_file = os.path.join(related_files_dir, f"{filename.replace('/', '_')}.json")
            
            with open(oscars_ds_path, "rb") as fh:
                dctx = zstd.ZstdDecompressor(max_window_size=2**31)
                with dctx.stream_reader(fh) as reader:
                    text_stream = io.TextIOWrapper(reader, encoding="utf-8")
                    for line in text_stream:
                        line = line.strip()
                        if not line:
                            continue
                        record = json.loads(line)
                        warc_target_uri = record['warc_headers']["warc-target-uri"].lower()
                        warc_record_id = record['warc_headers']["warc-record-id"]
                        warc_date = record['warc_headers']["warc-date"]
                        records[warc_target_uri].update({
                            "warc_record_id": warc_record_id,
                            "warc_date": _warc_timestamp(warc_date)
                        })
            
            with open(related_file, "w") as f:
                json.dump(records, f, indent=4, ensure_ascii=False)
            data['related_file'] = related_file
        except Exception as e:
            import traceback
            print(f"Error processing file {filename}: {e} \n{traceback.format_exc()}")
            return
        finally:
            dump_snapshots(snapshots, snapshot_path=snapshots_path)
            if oscars_ds_path and os.path.exists(oscars_ds_path):
                os.remove(oscars_ds_path)
                
                
def collect_offsets(snapshots_file):
    snapshots = load_snapshots(snapshots_file=snapshots_file)
    
    related_file = None 
    related_file_path = None
    for filename, data in list(snapshots.items())[:]:
        related_file_path = data.get("related_file", None)
        print(f"Extracting offsets for file: '{filename}'")
        
        try:
            snapshot_id = Path(filename).parts[1] 
            data['snapshot_id'] = snapshot_id
            all_snapshot_indexes = set(_list_indexes(snapshot_id))
            
            print(f"  Found {len(all_snapshot_indexes)} indexes for snapshot: '{snapshot_id}'")
            checked_indexes = set(data.get("checked_indexes", []))
            indexes_to_check = sorted(all_snapshot_indexes - checked_indexes)
            related_file = _related_file(related_file_path)
            
            # not_found = [k for k,v in related_file.items() if not v.get('offset', None)]
            # print(len(not_found), len(related_file))
            # return
            
            for index_url in indexes_to_check:
                full_url = f"https://data.commoncrawl.org/{index_url}"
                try:
                    _stream_index(full_url, related_file)
                except Exception as e:
                    print(f"Error requesting index {full_url}: {e}")
                    continue
                checked_indexes.add(index_url)
                data['checked_indexes'] = sorted(list(checked_indexes))
                dump_snapshots(snapshots, snapshot_path=snapshots_file)
                _dump_related_file(related_file_path, related_file)
            
            _print_summary(related_file)
        except Exception as e:
            import traceback
            print(f"Error processing file {filename}: {e} \n{traceback.format_exc()}")
            return
        finally:
            dump_snapshots(snapshots, snapshot_path=snapshots_file)
            if related_file and related_file_path:
                _dump_related_file(related_file_path, related_file)


def fetch_missing_offsets(snapshots_file, flush_every: int = 25):
    """Resolve missing offset/length/filename for URLs via Common Crawl CDX API.
    Best-effort; persists JSON after each successful resolution.
    """
    snapshots = load_snapshots(snapshots_file=snapshots_file)
    for filename, data in list(snapshots.items())[:3]:
        related_file_path = data.get("related_file", None)
        if not related_file_path:
            raise ValueError("Related file path not found for offset resolution")
        
        if not (snapshot_id := data.get("snapshot_id")):
            raise ValueError("Snapshot ID not found for offset resolution")
        
        related = _related_file(related_file_path)
        missing = [
            (url, details)
            for url, details in related.items()
            if not details.get("filename") or details.get("offset") is None or details.get("length") is None
        ]
        if not missing:
            print(f"No missing offsets found for '{filename}'")
        print(f"Resolving missing offsets via CDX API for '{filename}' (need {len(missing)})")
        resolved = 0
        dirty_since_flush = 0
        for url, details in track(missing, description="CDX lookup"):
            try:
                warc_date = details.get("warc_date")
                match = _cdx_lookup(snapshot_id, url, warc_date)
                if match:
                    details.update({
                        "length": int(match["length"]),
                        "offset": int(match["offset"]),
                        "filename": match["filename"],
                    })
                    resolved += 1
                    dirty_since_flush += 1
                    if dirty_since_flush >= flush_every:
                        _dump_related_file(related_file_path, related)
                        dirty_since_flush = 0
            except Exception as e:
                print(f"  Failed to resolve '{url}': {e}")
                continue
        if dirty_since_flush:
            _dump_related_file(related_file_path, related)
        print(f"  Resolved {resolved}/{len(missing)}")


def _print_summary(related_file):
    total = len(related_file)
    found = sum(1 for v in related_file.values() if 'offset' in v and v['offset'] is not None)
    print(f"  Summary: Found offsets for {found}/{total} digests.")


def _related_file(related_file):
    with open(related_file, "r") as f:
        return json.load(f)
    
    
def _list_indexes(snapshot_id):
    """Download index file from common crawl"""
    print(f"Downloading index for snapshot: '{snapshot_id}'")
    base = f"https://data.commoncrawl.org/crawl-data/CC-MAIN-{snapshot_id}"
    index_url = f"{base}/cc-index.paths.gz"
    
    resp = requests.get(index_url, stream=True)
    if resp.status_code != 200:
        raise RuntimeError(f"No index list found for {snapshot_id}: {index_url}")
    
    # decompress .gz
    data = gzip.decompress(resp.content)
    paths = data.decode("utf-8").strip().splitlines()
    
    # Each path is relative to https://data.commoncrawl.org/ f"https://data.commoncrawl.org/{p}"
    full_urls = [p for p in paths if p.endswith(".gz")]
    return full_urls


def _stream_index(index_url, related_file):
    with requests.get(index_url, stream=True) as r:
        r.raise_for_status()
        with gzip.GzipFile(fileobj=r.raw) as f:
            for line in track(f, f"  Scanning index '{index_url}'"):
                line = line.decode("utf-8").strip()
                parsed = _parse_sdx_line(line)
                if not (url := parsed.get('url')):
                    raise ValueError(f"  No url found in line: {line}")
                
                url = _clean(url)
                
                if data := related_file.get(url):
                    if not data.get('offset', None) or abs(_cc_timestamp(parsed['timestamp']) - data['warc_date']) < 86400:
                        data.update({
                            "length": int(parsed['length']),
                            "offset": int(parsed["offset"]),
                            "filename": parsed["filename"]
                        })
                        # print(f"  Found match of url '{url}'")


def _cc_timestamp(cc_timestamp):
    dt = datetime.strptime(cc_timestamp, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _warc_timestamp(warc_date):
    dt = datetime.strptime(warc_date, "%Y-%m-%dT%H:%M:%SZ")
    return int(dt.replace(tzinfo=timezone.utc).timestamp())

                        
def _parse_sdx_line(line):
    if "{" in line and "}" in line:
        return _parse_cdx_old(line)
    else:
        return _parse_cdx_new(line)
    
    
def _parse_cdx_old(line: str):
    try:
        urlkey, timestamp, json_part = line.strip().split(" ", 2)
        meta = json.loads(json_part)
        return {
            "urlkey": urlkey,
            "timestamp": timestamp,
            **meta
        }
    except Exception as e:
        print("Failed to parse line:", line[:200], e)
        return None
    
    
def _parse_cdx_new(line: str):
    parts = line.strip().split(" ")
    if len(parts) < 10:
        print("Incomplete CDX11 line:", line[:200])
        return None
    urlkey, timestamp, original, mimetype, status, digest, redirect, length, offset, filename = parts[:10]
    return {
        "urlkey": urlkey,
        "timestamp": timestamp,
        "url": original,
        "mimetype": mimetype,
        "status": status,
        "digest": digest,
        "redirect": redirect,
        "length": length,
        "offset": offset,
        "filename": filename
    }
    

def _clean(v):
    return None if v in ("-", "NONE", "", None) else v.strip().lower()
    

def _dump_related_file(related_file_path, data):
    part = f"{related_file_path}.part"
    with open(part, "w") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    os.rename(part, related_file_path)


def _cdx_lookup(snapshot_id: str, url: str, warc_date: int | None):
    """Query Common Crawl CDX API for one URL within a snapshot and choose the best match."""
    base = f"https://index.commoncrawl.org/CC-MAIN-{snapshot_id}-index"
    params = {
        "url": url,
        "output": "json",
        "fl": "timestamp,length,offset,filename,mimetype,status,digest,original",
    }
    resp = requests.get(base, params=params, timeout=30)
    if resp.status_code != 200:
        alt = base.replace("https://", "http://")
        resp = requests.get(alt, params=params, timeout=30)
    if resp.status_code != 200:
        return None
    lines = [l for l in resp.text.splitlines() if l.strip()]
    if not lines:
        return None
    items = []
    for line in lines:
        try:
            item = json.loads(line)
            if not item.get("filename") or not item.get("offset") or not item.get("length"):
                continue
            # Prefer HTML mimetype when available
            mt = str(item.get("mimetype") or "").lower()
            if mt and "html" not in mt:
                continue
            items.append(item)
        except Exception:
            continue
    if not items:
        return None
    if warc_date is None:
        return items[0]
    def score(it):
        try:
            ts = _cc_timestamp(it.get("timestamp"))
            return abs(ts - warc_date)
        except Exception:
            return 10**12
    items.sort(key=score)
    return items[0]
