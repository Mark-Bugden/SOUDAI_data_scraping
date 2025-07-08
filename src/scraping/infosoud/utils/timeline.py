from typing import Dict, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from config import IMPORTANT_EVENTS


def extract_case_timeline(url: str) -> Dict[str, Optional[str]]:
    """
    Extract key case event dates from the 'Průběh řízení' table on Infosoud.

    Only events listed in the IMPORTANT_EVENTS set are included. If the table
    is missing or malformed, returns an empty dictionary.

    Args:
        url (str): The Infosoud URL of the court case.

    Returns:
        dict: A dict of event names to date strings, or an empty dict if parsing fails.
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


def process_chunk(
    df_chunk: pd.DataFrame,
    progress_bar: Optional[tqdm] = None,
) -> pd.DataFrame:
    """
    Processes a chunk of the dataframe by scraping the Infosoud timeline
    for each case and merging it into the data.

    Timeline keys are prefixed with 'timeline_' to avoid column name collisions.
    """
    new_rows = []

    for _, row in df_chunk.iterrows():
        try:
            timeline = extract_case_timeline(row["infosoud_url"])
        except Exception as e:
            timeline = {
                k: None
                for k in [
                    "Zahájení řízení",
                    "Nařízení jednání",
                    "Vydání rozhodnutí",
                    "Vyřízení věci",
                    "Datum pravomocného ukončení věci",
                ]
            }
            print(f"Error encountered for URL {row['infosoud_url']}: {e}")

        # Prefix keys to avoid column clashes
        prefixed_timeline = {f"timeline_{k}": v for k, v in timeline.items()}
        new_row = row.to_dict()
        new_row.update(prefixed_timeline)
        new_rows.append(new_row)

        if progress_bar:
            progress_bar.update(1)

    return pd.DataFrame(new_rows)


def get_next_pending_chunk(
    df: pd.DataFrame, done_urls: set, chunk_size: int
) -> pd.DataFrame:
    """
    Return the next chunk of unprocessed cases from the full preprocessed dataframe.

    Args:
        df (pd.DataFrame): The full preprocessed DataFrame.
        done_urls (set): Set of infosoud_urls that have already been processed.
        chunk_size (int): Number of rows to return.

    Returns:
        pd.DataFrame: Next chunk of unprocessed rows.
    """
    df_pending = df[~df["infosoud_url"].isin(done_urls)]
    return df_pending.head(chunk_size)
