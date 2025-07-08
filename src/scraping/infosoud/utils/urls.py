from collections import OrderedDict
from typing import Optional
from urllib.parse import urlencode

import pandas as pd

from config import INFO_SOUD_BASE_URL
from scraping.infosoud.utils.court_data import court_lookup


def create_infosoud_URL(court_name: str, parsed_case: list) -> Optional[str]:
    """
    Construct a URL for the Infosoud web interface based on court name and parsed info.

    Args:
        court_name (str): The name of the court (must match an entry in the court map).
        parsed_case (list): Parsed case number in the form
            [cislo_senatu, druh_vec, bc_vec, rocnik, dash_num].

    Returns:
        Optional[str]: A fully formatted Infosoud URL, or None if construction fails.
    """
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
    """
    Add a column of constructed Infosoud URLs to the DataFrame.

    Expects each row to have a 'soud' (court name) and a
    'parsed_jednaciCislo' (parsed case number as a list).
    Returns a copy of the DataFrame with a new 'infosoud_url' column.
    """

    df = df.copy()
    df["infosoud_url"] = df.apply(
        lambda row: create_infosoud_URL(row["soud"], row["parsed_jednaciCislo"]), axis=1
    )
    return df
