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
def fetch_missing():
    import parse
    parse.fetch_missing_offsets(snapshot_file)


@app.command()
def export_parquet():
    import export
    export.export_snapshots_to_parquet(snapshot_file)


if __name__ == "__main__":
    app()
    
    