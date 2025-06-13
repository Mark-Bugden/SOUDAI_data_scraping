from pathlib import Path

import yaml


def load_patterns(filepath: Path) -> dict:
    with open(filepath, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
