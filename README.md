# OSCAR Tatar HTML Extractor

This project automates the extraction of Tatar-language HTML pages from the OSCAR Common Crawl derived dataset. It orchestrates the workflow end-to-end: discovering snapshot manifests on Hugging Face, resolving Common Crawl offsets via local CDX shards, downloading WARC files, and extracting original HTML.

## Features

- Indexes available Tatar snapshot files (`tt_meta*.jsonl.zst`) from the `oscar-corpus/community-oscar` Hugging Face dataset
- Extracts target URIs and WARC metadata from OSCAR Zstandard-compressed JSONL shards
- Resolves Common Crawl `offset`, `length`, and WARC `filename` via the CC index
- Downloads HTML bodies by byte-range streaming from Common Crawl
- Fills gaps using the Common Crawl CDX API for stubborn URLs
- Exports per-snapshot Parquet files containing URL, offsets, filenames, HTML content, and markdown (via Trafilatura)
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

Entry point: `python -m app.cli`. Default workdir: `~/.oscar` (override via `config.yaml` or `OSCAR_APP_DIR`). Key commands:

1) Ingest OSCAR shards into SQLite  
`python -m app.cli ingest`

1) Resolve offsets via local CDX shards (no rate limits)  
`python -m app.cli resolve-offsets-local --snapshot CC-MAIN-2014-42`  
Downloads `indexes/cdx-*.gz` to `~/.oscar/app/indexes/<snapshot>/` (cached), scans locally, and updates offsets/filenames.

1) Prepare WARC path list for cc-downloader  
`python -m app.cli prepare-downloads`

1) Download WARC files (bundled cc-downloader binary, patched to auto-detect plain/gz path files)  
`python -m app.cli download-warcs`

1b) Download HTML via HTTP Range (skip full WARCs, resumable)  
`python -m app.cli download-ranges [--snapshot SNAP] [--limit N]`

1c) Download a single WARC byte range (ad hoc)  
`python -m app.cli download-range <filename> <offset> <length> [--dest OUT]`

1) Extract HTML from downloaded WARCs  
`python -m app.cli extract-html`

1) Progress snapshot  
`python -m app.cli stats`

1) Export Parquet with markdown  
`python -m app.cli export-parquet [--snapshot SNAP] [--limit N] [--split 1024]`

## Project Structure

```
app/
├── cli.py         # Typer entrypoint
├── cdx.py         # Local CDX shard resolver
├── downloader.py  # cc-downloader integration + HTML extraction
├── hf.py          # Hugging Face shard listing
├── ingest.py      # OSCAR shard ingest into SQLite
├── db.py          # SQLite helpers
└── config.py      # Settings loader (paths, tokens, cc-downloader)
```

Persistent workspace (`~/.oscar/app/`):

- `state.sqlite` – URLs, offsets, filenames, download status
- `shards/` – Downloaded OSCAR shards
- `indexes/<snapshot>/` – Cached CDX shards (safe to delete after resolving)
- `warc/` – Downloaded WARC files
- `html/` – Extracted HTML documents

## Notes and Tips

- The Common Crawl requests can be bandwidth-heavy. Consider running `collect_offsets` and `download` in batches or with resumed snapshots.
- HTML healing uses BeautifulSoup (`html5lib` parser) to tolerate malformed markup. Adjust `heal_html` if you need raw bytes or a different parser.
- Markdown conversion is performed using Trafilatura with `output_format="markdown"` and `with_metadata=True`. If conversion fails for a document, the markdown field will be `None`.
- Parquet export includes full HTML bodies and markdown by default; modify `export.export_parquet` if you prefer to store references only.
- Output filenames drop the `CC-MAIN-` prefix. If `--split` is set, it is interpreted as megabytes and files are split per snapshot as `<snapshot>_part0000.parquet`, `<snapshot>_part0001.parquet`, etc.
- If you interrupt the pipeline, re-running commands resumes where they left off thanks to the JSON state files.

## License

MIT
