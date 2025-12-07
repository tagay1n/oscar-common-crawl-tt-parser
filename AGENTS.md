# Project Notes for Future Agents

## Overview
- Repository: OSCAR Tatar HTML extractor. Pulls TT snapshot metadata from Hugging Face, resolves Common Crawl offsets, downloads HTML, and exports Parquet.
- Primary entrypoint: `python -m app.cli`.
- Workdir defaults to `~/.oscar/app` (configurable via `config.yaml` or `OSCAR_APP_DIR`).

## Recent Changes (this session)
- Patched bundled `cc-downloader` binary (v0.6.1) to auto-detect plain vs gzipped path files to avoid “invalid gzip header” panic. Binary lives at `cc-downloader/cc-downloader`.
- Added a local CDX resolver to bypass public API rate limits:
  - Command: `python -m app.cli resolve-offsets-local [--snapshot SNAP] [--limit N]`
  - Downloads snapshot CDX shards (`indexes/cdx-*.gz`) to `~/.oscar/app/indexes/<snapshot>/` if absent, then scans them locally and updates SQLite offsets/filenames.
  - Old API resolver still available as `resolve-offsets`.
- Settings now include `index_dir` (`~/.oscar/app/indexes/`).

## Key Paths
- Config: `config.yaml`
- SQLite: `~/.oscar/app/state.sqlite`
- Shards: `~/.oscar/app/shards/`
- CDX shards cache: `~/.oscar/app/indexes/<snapshot>/` (user may delete manually after processing)
- Downloaded WARC: `~/.oscar/app/warc/`
- Extracted HTML: `~/.oscar/app/html/`

## CLI Reminders
- `python -m app.cli ingest` — ingest TT shards into SQLite.
- `python -m app.cli resolve-offsets-local` — preferred offset resolver (no rate limits); use `--snapshot` to target a specific crawl.
- `python -m app.cli prepare-downloads` — write WARC paths for cc-downloader.
- `python -m app.cli download-warcs` — run cc-downloader on the path file.
- `python -m app.cli extract-html` — extract HTML from downloaded WARCs.
- `python -m app.cli stats` — quick status.

## Notes
- CDX shards are cached; reruns skip downloads. User plans to delete cached shards manually to save space.
- If disk is constrained, a future improvement is a stream-and-delete mode for CDX shards.
