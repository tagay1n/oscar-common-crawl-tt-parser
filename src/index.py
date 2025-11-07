from huggingface_hub import list_repo_files, login
import json
import utils
import os

def index_snapshots_file(snapshot_file):
    if os.path.exists(snapshot_file):
        with open(snapshot_file, "r") as f:
            relevant_files = json.load(f)
    else:
        relevant_files = {}

    config = utils.read_config()
    login(token=config['hf']['token'])
    print("Listing relevant files in the repository")
    _relevant_files = {f: {} for f in list_repo_files(config['hf']['repo'], repo_type="dataset") if "tt_meta" in f and f.endswith(".jsonl.zst")}
    
    for _k, _v in _relevant_files.items():
        if _k in relevant_files:
            relevant_files[_k].update(_v)
        else:
            relevant_files[_k] = _v
            
    with open(snapshot_file, "w") as f:
        json.dump(relevant_files, f, indent=4, ensure_ascii=False, sort_keys=True)
    print(f"Found {len(relevant_files)} files")