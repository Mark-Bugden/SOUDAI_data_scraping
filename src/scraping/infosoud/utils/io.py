import json
import os
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from scraping.infosoud.utils.filtering import filter_out_bad_decisions
from scraping.infosoud.utils.parsing import add_parsed_jednaciCislo
from scraping.infosoud.utils.urls import add_infosoud_urls


def load_or_create_preprocessed(preprocessed_path: str, raw_path: str) -> pd.DataFrame:
    """
    Load the preprocessed decisions CSV file, or generate it from raw JSON files
    if it doesn't exist.

    Args:
        preprocessed_path (str): Path to the preprocessed CSV file.
        raw_path (str): Base directory containing raw JSON decision data.

    Returns:
        pd.DataFrame: The loaded or newly created preprocessed dataframe.
    """
    if os.path.exists(preprocessed_path):
        return pd.read_csv(preprocessed_path)

    df_raw_json = load_all_decisions(base_path=raw_path)
    df_filtered = filter_out_bad_decisions(df_raw_json)
    df_parsed = add_parsed_jednaciCislo(df_filtered)
    df_preprocessed = add_infosoud_urls(df_parsed)
    df_preprocessed.to_csv(preprocessed_path, index=False)
    print("Preprocessed dataframe created and saved.")
    return df_preprocessed


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


def load_done_urls(checkpoint_csv_path: Path) -> set:
    """
    Load a set of previously processed Infosoud URLs from a checkpoint CSV file.

    Returns an empty set if the file doesn't exist or lacks the 'infosoud_url' column.
    """

    if os.path.exists(checkpoint_csv_path):
        df_checkpoint = pd.read_csv(checkpoint_csv_path, dtype=str)
        if "infosoud_url" in df_checkpoint.columns:
            return set(df_checkpoint["infosoud_url"])
    return set()
