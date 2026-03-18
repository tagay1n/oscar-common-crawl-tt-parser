# OSCAR Tatar HTML Extractor

This project automates the extraction of Tatar-language HTML pages from the OSCAR Common Crawl derived dataset. It orchestrates the workflow end-to-end: discovering snapshot manifests on Hugging Face, resolving Common Crawl offsets via local CDX shards, downloading WARC files, and extracting original HTML.

## Features

- Indexes available Tatar snapshot files (`tt_meta*.jsonl.zst`) from the `oscar-corpus/community-oscar` Hugging Face dataset
- Extracts target URIs and WARC metadata from OSCAR Zstandard-compressed JSONL shards
- Resolves Common Crawl `offset`, `length`, and WARC `filename` via locally scanned CDX shards (`resolve-offsets-local`)
- Supports an optional online CDX API resolver (`resolve-offsets`) when needed
- Supports two download paths:
  - full WARC download via bundled `cc-downloader` + local extraction
  - direct HTTP Range fetch per record (`download-ranges`)
- Exports per-snapshot Parquet files containing URL, offsets, filenames, HTML content, and markdown (via Trafilatura)
- Provides status and troubleshooting helpers through a Typer CLI

## Installation

```bash
git clone https://github.com/tagay1n/tat-data-parser.git oscar-corpus-extractor
cd oscar-corpus-extractor
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Configuration

Set runtime options in `config.yaml`:

```yaml
hf:
  token: ""   # optional if HF_TOKEN env var is set
  repo: oscar-corpus/community-oscar

app:
  workdir: ~/.oscar

cc_downloader:
  binary: cc-downloader/cc-downloader
  threads: 2

cdx:
  min_delay: 1.5
  max_retries: 4
  timeout: 30
```

Token can also be provided by environment variable:

```bash
export HF_TOKEN=YOUR_HF_TOKEN
```

> Keep secrets out of git history. Do not commit real tokens in `config.yaml`.

## CLI Overview

Entry point: `python -m app.cli`. Default workdir: `~/.oscar` (override via `config.yaml` `app.workdir` or `OSCAR_APP_DIR`). Key commands:

1. Ingest OSCAR shards into SQLite  
`python -m app.cli ingest [--force]`

2. Resolve offsets via local CDX shards (preferred; no public API rate limits)  
`python -m app.cli resolve-offsets-local --snapshot CC-MAIN-2014-42`  
Downloads `indexes/cdx-*.gz` to `~/.oscar/indexes/<snapshot>/` (cached), scans locally, and updates offsets/filenames.

3. Optional fallback resolver via online CDX API  
`python -m app.cli resolve-offsets [--limit N]`

4. Prepare WARC path list for cc-downloader  
`python -m app.cli prepare-downloads`

5. Download WARC files (bundled cc-downloader binary)  
`python -m app.cli download-warcs`

6. Download HTML via HTTP Range (skip full WARCs)  
`python -m app.cli download-ranges [--snapshot SNAP] [--limit N]`

7. Download a single WARC byte range (ad hoc)  
`python -m app.cli download-range <filename> <offset> <length> [--dest OUT]`

8. Extract HTML from downloaded WARCs  
`python -m app.cli extract-html`

9. Progress snapshot  
`python -m app.cli stats`

10. Export Parquet with markdown  
`python -m app.cli export-parquet [--snapshot SNAP] [--limit N] [--split 1024]`

## Project Structure

```
app/
├── cli.py         # Typer entrypoint
├── cdx.py         # Local CDX shard resolver
├── downloader.py  # cc-downloader integration + HTML extraction
├── export.py      # Parquet export logic
├── hf.py          # Hugging Face shard listing
├── ingest.py      # OSCAR shard ingest into SQLite
├── db.py          # SQLite helpers
└── config.py      # Settings loader (paths, tokens, cc-downloader)
```

Persistent workspace (`~/.oscar/`):

- `state.sqlite` – URLs, offsets, filenames, download status
- `shards/` – Downloaded OSCAR shards
- `indexes/<snapshot>/` – Cached CDX shards (safe to delete after resolving)
- `warc/` – Downloaded WARC files
- `warc/parts/` – Optional single-range downloads from `download-range`
- `html/` – Extracted HTML documents
- `parquet/` – Exported Parquet files

## Notes and Tips

- The Common Crawl requests can be bandwidth-heavy. Consider running `resolve-offsets-local`, downloads, and exports in batches.
- Markdown conversion is performed using Trafilatura with `output_format="markdown"` and `with_metadata=True`. If conversion fails for a document, the markdown field will be `None`.
- Parquet export includes full HTML bodies and markdown by default; modify `export.export_parquet` if you prefer to store references only.
- Output filenames drop the `CC-MAIN-` prefix. If `--split` is set, it is interpreted as megabytes and files are split per snapshot as `<snapshot>_part0000.parquet`, `<snapshot>_part0001.parquet`, etc.
- If you interrupt the pipeline, re-running commands resumes where they left off thanks to the SQLite state.

## License

MIT
