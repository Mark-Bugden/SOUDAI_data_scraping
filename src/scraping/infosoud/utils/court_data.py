import yaml

from config import REFERENCES_DIR

COURT_MAP_PATH = REFERENCES_DIR / "court_map.yaml"

with open(COURT_MAP_PATH, "r", encoding="utf-8") as f:
    court_map = yaml.safe_load(f)

court_lookup = {
    entry["name"]: entry
    for entry in court_map
    if isinstance(entry, dict) and "name" in entry
}
