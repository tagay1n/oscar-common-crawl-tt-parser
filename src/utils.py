import yaml
import json
import os


def read_config(config_path: str = "config.yaml") -> dict:
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)
    
    
def load_snapshots(snapshots_file):
    with open(snapshots_file, "r") as f:
        return json.load(f)
    

def dump_snapshots(snapshots, snapshot_path):
    part = f"{snapshot_path}.part"
    with open(part, "w") as f:
        json.dump(snapshots, f, indent=4, ensure_ascii=False)
    os.rename(part, snapshot_path)