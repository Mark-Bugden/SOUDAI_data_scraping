# ==========================================
# Imports and Constants
# ==========================================
import json
import os
from collections import OrderedDict
from pathlib import Path
from typing import Dict, Optional
from urllib.parse import urlencode

import pandas as pd
import requests
import yaml
from bs4 import BeautifulSoup
from tqdm import tqdm

from config import REFERENCES_DIR

IMPORTANT_EVENTS = {
    "Zahájení řízení",
    "Vydání rozhodnutí",
    "Vyřízení věci",
    "Nařízení jednání",
    "Datum pravomocného ukončení věci",
    "Skončení věci",
}

INFO_SOUD_BASE_URL = "https://infosoud.justice.cz/InfoSoud/public/search.do"
COURT_MAP_PATH = REFERENCES_DIR / "court_map.yaml"

with open(COURT_MAP_PATH, "r", encoding="utf-8") as f:
    court_map = yaml.safe_load(f)

court_lookup = {
    entry["name"]: entry
    for entry in court_map
    if isinstance(entry, dict) and "name" in entry
}

# ==========================================
# Loading functions
# ==========================================


def load_done_urls(checkpoint_csv_path: Path) -> set:
    if os.path.exists(checkpoint_csv_path):
        df_checkpoint = pd.read_csv(checkpoint_csv_path, dtype=str)
        if "infosoud_url" in df_checkpoint.columns:
            return set(df_checkpoint["infosoud_url"])
    return set()


def load_all_decisions(base_path: Path) -> pd.DataFrame:
    """Loads and aggregates all JSON decision records from nested folders."""
    records = []

    for json_file in tqdm(base_path.rglob("page*.json"), desc="Loading JSON files"):
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            records.extend(data.get("items", []))
        except Exception as e:
            print(f"Failed to load {json_file}: {e}")

    return pd.DataFrame(records)


# ==========================================
# Filtering pipeline
# ==========================================
def filter_out_bad_decisions(df: pd.DataFrame) -> pd.DataFrame:
    return df.pipe(filter_out_bad_court_names).pipe(filter_out_bad_case_numbers)


def filter_out_bad_court_names(df: pd.DataFrame) -> pd.DataFrame:
    is_known = df["soud"].isin(court_lookup.keys())
    filtered = df[is_known].copy().reset_index(drop=True)
    n_removed = (~is_known).sum()
    if n_removed > 0:
        print(
            f"Filtered out {n_removed} decisions with unknown or missing court names."
        )
    return filtered


def filter_out_bad_case_numbers(df: pd.DataFrame) -> pd.DataFrame:
    is_valid = df["jednaciCislo"].apply(_parse_jednaciCislo).notna()
    filtered = df[is_valid].copy().reset_index(drop=True)
    n_removed = (~is_valid).sum()
    if n_removed > 0:
        print(f"Filtered out {n_removed} rows with invalid jednaciCislo values.")
    return filtered


# ==========================================
# Parsing / enrichment
# ==========================================
def _parse_jednaciCislo(case_number: str) -> Optional[list]:
    if not isinstance(case_number, str):
        return None
    try:
        parts = case_number.split(" ")
        if len(parts) != 3:
            return None
        cislo_senatu = int(parts[0])
        druh_vec = parts[1]
        third = parts[2]
        if "-" in third:
            slash_part, dash = third.split("-")
            dash_num = int(dash)
        else:
            slash_part = third
            dash_num = None
        bc_vec, rocnik = slash_part.split("/")
        bc_vec = int(bc_vec)
        rocnik = int(rocnik)
        return [cislo_senatu, druh_vec, bc_vec, rocnik, dash_num]
    except Exception:
        return None


def add_parsed_jednaciCislo(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["parsed_jednaciCislo"] = df["jednaciCislo"].apply(_parse_jednaciCislo)
    return df


# ==========================================
# Infosoud url construction
# ==========================================
def create_infosoud_URL(court_name: str, parsed_case: list) -> Optional[str]:
    if parsed_case is None:
        return None
    court = court_lookup.get(court_name)
    if not court or not court.get("typSoudu"):
        return None
    try:
        cislo_senatu, druh_vec, bc_vec, rocnik, dash_num = parsed_case
        query = OrderedDict(
            {
                "type": "spzn",
                "typSoudu": court["typSoudu"],
                "cisloSenatu": cislo_senatu,
                "druhVec": druh_vec,
                "bcVec": bc_vec,
                "rocnik": rocnik,
                "spamQuestion": "23",
                "agendaNc": "CIVIL",
            }
        )
        if "krajOrg" in court:
            query["krajOrg"] = court["krajOrg"]
        if "Org" in court:
            query["org"] = court["Org"]
        return f"{INFO_SOUD_BASE_URL}?{urlencode(query)}"
    except Exception as e:
        print(
            f"[ERROR] Could not build URL for court '{court_name}' "
            f"with parsed_case {parsed_case}: {e}"
        )
        return None


def add_infosoud_urls(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["infosoud_url"] = df.apply(
        lambda row: create_infosoud_URL(row["soud"], row["parsed_jednaciCislo"]), axis=1
    )
    return df


# ==========================================
# Checkpointing and stop/start
# ==========================================


class StopFlag:
    """Thread-safe stop signal shared between main thread and input listener."""

    def __init__(self):
        self._stop = False

    def request_stop(self) -> None:
        self._stop = True

    def is_requested(self) -> bool:
        return self._stop


def listen_for_quit(stop_flag: StopFlag) -> None:
    """Listen for user typing 'q' to request a graceful stop."""
    while True:
        user_input = (
            input("Type 'q' and press Enter to stop after current chunk: \n")
            .strip()
            .lower()
        )
        if user_input == "q":
            tqdm.write("Stop requested. Will exit after current chunk.")
            stop_flag.request_stop()
            break


def deduplicate_checkpoint(checkpoint_csv_path: Path):
    if not checkpoint_csv_path.exists():
        return
    df = pd.read_csv(checkpoint_csv_path, dtype=str)
    deduped = df.drop_duplicates(subset="infosoud_url")
    if len(deduped) < len(df):
        tqdm.write(f"Removed {len(df) - len(deduped)} duplicate rows from checkpoint.")
        deduped.to_csv(checkpoint_csv_path, index=False)


def validate_checkpoint(df_preprocessed: pd.DataFrame, checkpoint_csv_path: Path):
    if not os.path.exists(checkpoint_csv_path):
        print("No checkpoint found, skipping validation.")
        return

    df_checkpoint = pd.read_csv(checkpoint_csv_path, dtype=str)
    urls_in_checkpoint = set(df_checkpoint["infosoud_url"])

    # should be subset of preprocessed
    all_urls = set(df_preprocessed["infosoud_url"])
    missing = urls_in_checkpoint - all_urls

    if missing:
        raise ValueError(
            f"Checkpoint file has URLs not present in the source data: {missing}"
        )

    duplicates = df_checkpoint["infosoud_url"][
        df_checkpoint["infosoud_url"].duplicated()
    ]
    if not duplicates.empty:
        raise ValueError(
            f"Checkpoint file has duplicate infosoud_url entries:\n{duplicates}"
        )

    print(f"Checkpoint OK: {len(df_checkpoint)} valid entries.")


# ==========================================
# Timeline extraction
# ==========================================


def extract_case_timeline(url: str) -> Dict[str, Optional[str]]:
    """
    Extracts key case dates from the Infosoud 'Průběh řízení' table.

    Returns a dict with keys like 'Zahájení řízení', etc.
    """
    response = requests.get(url)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    heading = soup.find(string="Průběh řízení")
    if not heading:
        return {}

    table = heading.find_parent().find_next("table")
    if not table:
        return {}

    rows = table.find_all("tr")[1:]
    timeline = {}

    for row in rows:
        cells = row.find_all("td")
        if len(cells) != 2:
            continue
        a_tag = cells[0].find("a")
        if not a_tag:
            continue
        event = a_tag.get_text(strip=True)
        if event in IMPORTANT_EVENTS:
            timeline[event] = cells[1].get_text(strip=True)

    return timeline
