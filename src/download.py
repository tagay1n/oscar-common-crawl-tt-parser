import json
from utils import load_snapshots
import requests, gzip, io
# from warcio.archiveiterator import ArchiveIterator


def download(snapshot_file):
    snapshots = load_snapshots(snapshot_file)
    
    for filename, data in snapshots.items():
        if not data.get("offsets_extracted", False):
            continue
        print(filename)
        try:
            _process_snapshot(data)
        except:
            pass 


def _process_snapshot(data):
    related_file = _related_file(data.get("related_file", None))
    for url, details in related_file:
        if details.get("downloaded", False):
            continue

def _related_file(related_file):
    with open(related_file, "r") as f:
        return json.load(f)
    
