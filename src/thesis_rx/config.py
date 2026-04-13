from pathlib import Path
import yaml


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ensure_output_dir(cfg: dict) -> Path:
    outdir = Path(cfg["project"]["output_dir"])
    outdir.mkdir(parents=True, exist_ok=True)
    return outdir
