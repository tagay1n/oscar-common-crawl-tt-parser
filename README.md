# OSCAR Tatar HTML Extractor

This project automates the extraction of Tatar-language HTML pages from the OSCAR Common Crawl derived dataset. It orchestrates the workflow end-to-end: discovering snapshot manifests on Hugging Face, cross-referencing Common Crawl indexes to locate raw WARC segments, downloading HTML content, and exporting curated records to Apache Parquet.

## Features

- Indexes available Tatar snapshot files (`tt_meta*.jsonl.zst`) from the `oscar-corpus/community-oscar` Hugging Face dataset
- Extracts target URIs and WARC metadata from OSCAR Zstandard-compressed JSONL shards
- Resolves Common Crawl `offset`, `length`, and WARC `filename` via the CC index
- Downloads HTML bodies by byte-range streaming from Common Crawl
- Fills gaps using the Common Crawl CDX API for stubborn URLs
- Exports per-snapshot Parquet files containing URL, offsets, filenames, and HTML content
- Provides status and troubleshooting helpers through a Typer CLI

## Installation

```bash
git clone https://github.com/<your-user>/oscar-corpus-extractor.git
cd oscar-corpus-extractor
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Configuration

Copy `config.yaml` to provide a Hugging Face access token and the target dataset:

```yaml
hf:
  token: YOUR_HF_TOKEN
  repo: oscar-corpus/community-oscar
```

> The token must have access to the OSCAR community dataset and enough bandwidth to download snapshot manifests.

## CLI Overview

The entry point is `python -m src.main`. All data artifacts (snapshots index, per-snapshot JSON, downloaded HTML, exported Parquet) default to the work directory `~/.oscar/`.

### 1. Index snapshot files

Lists relevant files in the Hugging Face dataset, storing results in `~/.oscar/snapshots.json`.

```bash
python -m src.main index_snapshot_files
```

### 2. Extract URIs from OSCAR shards

Downloads each ZST shard, extracts `warc-target-uri` and metadata, and stores per-snapshot JSON under `~/.oscar/related_files/`.

```bash
python -m src.main collect_uris
```

### 3. Resolve offsets via Common Crawl index

Streams Common Crawl index paths, locating `offset`, `length`, and `filename` matches for each URI. Progress persists to `snapshots.json` and the per-snapshot JSON files.

```bash
python -m src.main collect_offsets
```

### 4. Download HTML bodies

Per snapshot, range-requests the WARC segment from Common Crawl, extracts the HTTP response, heals malformed HTML, and saves files under `~/.oscar/html/<snapshot_id>/`.

```bash
python -m src.main download
```

### 5. Fill remaining gaps (optional)

Uses the Common Crawl CDX API to backfill missing offsets/filenames that were not found in the index traversal. Adjust `--flush-every` for large batches.

```bash
python -m src.main fetch_missing --flush-every 50
```

### 6. Export to Parquet

Reads the per-snapshot JSON + downloaded HTML files and writes one Parquet file per snapshot to `~/.oscar/parquet/`. Each row contains:

| Column | Description |
|--------|-------------|
| `url` | Original URL (lower-cased) |
| `offset` | Common Crawl byte offset |
| `length` | Byte length of the response |
| `filename` | WARC filename on Common Crawl |
| `html` | Downloaded and healed HTML content |

```bash
python -m src.main export_parquet
```

## Project Structure

```
src/
├── main.py        # Typer CLI entry point
├── index.py       # Hugging Face snapshot discovery
├── parse.py       # URI extraction & index matching
├── download.py    # Common Crawl download logic
├── export.py      # Parquet writer
├── utils.py       # Config + snapshot IO helpers
└── ...
```

Persistent workspace (`~/.oscar/`):

- `snapshots.json` – Global state per snapshot file
- `related_files/*.json` – Per-snapshot URL metadata (offsets, filenames, download status)
- `html/<snapshot_id>/` – Downloaded HTML documents
- `parquet/<snapshot_id>.parquet` – Final exported datasets

## Notes and Tips

- The Common Crawl requests can be bandwidth-heavy. Consider running `collect_offsets` and `download` in batches or with resumed snapshots.
- HTML healing uses BeautifulSoup (`html5lib` parser) to tolerate malformed markup. Adjust `heal_html` if you need raw bytes or a different parser.
- Parquet export includes full HTML bodies by default; modify `export.export_snapshots_to_parquet` if you prefer to store references only.
- If you interrupt the pipeline, re-running commands resumes where they left off thanks to the JSON state files.

## License

MIT (or specify your license of choice).


