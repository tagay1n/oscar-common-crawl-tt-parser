import json
import requests
import os
import io
from urllib.parse import urlparse
from rich import print
from rich.progress import track
from warcio.archiveiterator import ArchiveIterator
import zlib
from bs4 import BeautifulSoup

from utils import load_snapshots


def download(snapshot_file):
    snapshots = load_snapshots(snapshot_file)

    for filename, data in list(snapshots.items())[6:8]:
        print(f"[bold green]Processing snapshot {filename}[/bold green]")
        try:
            _process_snapshot(data)
        except Exception as e:
            print(f"[red]Error processing snapshot {filename}: {e}[/red]")


def _process_snapshot(data):
    related_file_path = data.get("related_file")
    if not related_file_path:
        raise ValueError("Related file path not found for index extraction")

    snapshot_id = data.get("snapshot_id")
    out_root = os.path.expanduser("~/.oscar/html")
    out_dir = os.path.join(out_root, snapshot_id)
    os.makedirs(out_dir, exist_ok=True)

    related = _related_file(related_file_path)

    for url, details in track(related.items(), description="Downloading URLs"):
        if details.get("saved_path"):
            continue

        filename = details.get("filename")
        offset = details.get("offset")
        length = details.get("length")
        if not filename or offset is None or length is None:
            # print(f"Missing filename/offset/length for url: {url}, details: {details}")
            continue

        start = int(offset)
        end = start + int(length) + 4095  # pad a bit to ensure gzip footer
        headers = {"Range": f"bytes={start}-{end}"}
        cc_url = f"https://data.commoncrawl.org/{filename}"

        try:
            r = requests.get(cc_url, headers=headers, stream=True, timeout=60)
            r.raise_for_status()

            buf = io.BytesIO()
            for chunk in r.iter_content(chunk_size=64 * 1024):
                buf.write(chunk)
            buf.seek(0)

            # ✅ Parse directly with ArchiveIterator (no extra gzip)
            it = ArchiveIterator(buf)
            payload = None

            for record in it:
                if record.rec_type == "response":
                    # if record.http_headers:
                        # content_type = record.http_headers.get_header("Content-Type")
                    payload = record.content_stream().read()
                    # Handle gzip-encoded HTTP body
                    encoding = (
                        record.http_headers.get_header("Content-Encoding")
                        if record.http_headers
                        else None
                    )
                    if encoding and "gzip" in encoding.lower():
                        try:
                            payload = zlib.decompress(payload, 16 + zlib.MAX_WBITS)
                        except Exception:
                            pass
                    break

            if payload is None:
                raise ValueError("No WARC response record found in segment")

            # 🩹 Heal HTML before saving
            healed_html = heal_html(payload)

            out_name = _safe_filename(url, details.get("digest"))
            ext = ".html"
            out_path = os.path.join(out_dir, out_name + ext)

            with open(out_path, "w", encoding="utf-8") as f:
                f.write(healed_html)

            details["saved_path"] = out_path
            _dump_related_file(related_file_path, related)

            # print(f"[green]Saved {url} -> {out_path} ({len(healed_html)} chars, {content_type})[/green]")

        except Exception as e:
            print(f"[red]Error downloading {url}: {e}[/red]")
            continue


# 🩹---------------------------------------------------------------
# HTML Healing utilities
# ---------------------------------------------------------------

def heal_html(raw_html: bytes) -> str:
    """Decode and fix malformed HTML using BeautifulSoup(html5lib)."""
    try:
        text = raw_html.decode("utf-8")
    except UnicodeDecodeError:
        text = raw_html.decode("latin-1", errors="replace")

    soup = BeautifulSoup(text, "html5lib")
    healed = soup.prettify(formatter="html")

    # Ensure closing tag
    if not healed.strip().lower().endswith("</html>"):
        healed += "\n</html>"
    return healed


# ---------------------------------------------------------------

def _related_file(related_file):
    with open(related_file, "r") as f:
        return json.load(f)


def _dump_related_file(related_file_path, data):
    tmp = f"{related_file_path}.part"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    os.replace(tmp, related_file_path)


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
