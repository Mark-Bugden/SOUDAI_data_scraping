import os
from pathlib import Path
from typing import Optional

import pandas as pd
from tqdm import tqdm

from scraping.infosoud.utils.timeline import process_chunk


def process_and_update_checkpoint(
    df_chunk: pd.DataFrame,
    checkpoint_csv_path: Path,
    progress_bar: Optional[tqdm] = None,
) -> pd.DataFrame:
    """
    Process a chunk of decisions by extracting timeline information and updating
    the checkpoint file. Returns the processed chunk (with timeline fields added).

    If the checkpoint already exists, the function merges the new data with existing
    rows, removes duplicates (based on `infosoud_url`), and overwrites the file.

    Args:
        df_chunk (pd.DataFrame): A chunk of decisions to process.
        checkpoint_csv_path (Path): Path to the CSV checkpoint file.
        progress_bar (Optional[tqdm]): TQDM progress bar to update after each row.

    Returns:
        pd.DataFrame: The processed chunk with timeline information.
    """
    df_new = process_chunk(df_chunk, progress_bar)

    if os.path.exists(checkpoint_csv_path):
        df_existing = pd.read_csv(checkpoint_csv_path, dtype=str)
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        df_combined = df_combined.drop_duplicates(subset="infosoud_url")
        df_combined.to_csv(checkpoint_csv_path, index=False)
    else:
        df_new.to_csv(checkpoint_csv_path, index=False)

    return df_new


def deduplicate_checkpoint(checkpoint_csv_path: Path):
    """
    Remove duplicate rows from the checkpoint file based on 'infosoud_url'.

    If the checkpoint does not exist, does nothing. Otherwise, overwrites the file
    with a deduplicated version (if needed).
    """

    if not checkpoint_csv_path.exists():
        return
    df = pd.read_csv(checkpoint_csv_path, dtype=str)
    deduped = df.drop_duplicates(subset="infosoud_url")
    if len(deduped) < len(df):
        tqdm.write(f"Removed {len(df) - len(deduped)} duplicate rows from checkpoint.")
        deduped.to_csv(checkpoint_csv_path, index=False)


def validate_checkpoint(df_preprocessed: pd.DataFrame, checkpoint_csv_path: Path):
    """
    Validate the consistency of a checkpoint file against preprocessed data.

    Checks:
    - All 'infosoud_url' entries in the checkpoint must exist in df_preprocessed.
    - No duplicate 'infosoud_url' values in the checkpoint.

    Args:
        df_preprocessed (pd.DataFrame): The full preprocessed dataset containing
            'infosoud_url'.
        checkpoint_csv_path (Path): Path to the CSV checkpoint file.

    Returns:
        bool: True if the checkpoint is valid or does not exist.

    Raises:
        ValueError: If the checkpoint contains unknown or duplicate URLs.
    """

    if not os.path.exists(checkpoint_csv_path):
        print("No checkpoint found, skipping validation.")
        return True

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
    return True
