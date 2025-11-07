from rich import print
import os
import typer


workdir = os.path.expanduser("~/.oscar")
snapshot_file = os.path.join(workdir, "snapshots.json")
related_files_dir = os.path.join(workdir, "related_files")
os.makedirs(related_files_dir, exist_ok=True)


app = typer.Typer(context_settings={"help_option_names": ["-h", "--help"]})


@app.command()
def index_snapshot_files():
    import index
    index.index_snapshots_file(snapshot_file)

    
@app.command()
def collect_uris():
    import parse
    parse.collect_uris(snapshot_file, related_files_dir)
    
    
@app.command()
def collect_offsets():
    import parse
    parse.collect_offsets(snapshot_file)
    
    
@app.command()
def download():
    import download
    download.download(snapshot_file)
    
    
@app.command()
def fetch_missing(flush_every: int = typer.Option(25, help="Persist JSON after this many updates")):
    import parse
    parse.fetch_missing_offsets(snapshot_file, flush_every=flush_every)


@app.command()
def export_parquet():
    import export
    export.export_snapshots_to_parquet(snapshot_file)



if __name__ == "__main__":
    app()
    
    
# snapshot: file_name(id, str), uri_extracted(bool), snapshot_id(str), offsets_extracted(bool), downloaded(bool), archived(bool)
# checked_index: name(id, str), snapshot(snapshot's file name, secondary key)
# records: uri(id, str), offset(int), length(int),  filename(int), warc_record_id(str), warc_date(int), local_file(str)
    