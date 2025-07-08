import pandas as pd

from scraping.infosoud.utils.court_data import court_lookup
from scraping.infosoud.utils.parsing import parse_jednaciCislo


def filter_out_bad_decisions(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows with unknown courts or invalid case numbers."""
    return df.pipe(filter_out_bad_court_names).pipe(filter_out_bad_case_numbers)


def filter_out_bad_court_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter out rows with unknown or invalid court names.

    Keeps only rows where the 'soud' column matches a known court
    name defined in the court lookup table (from court_map.yaml).

    Args:
        df (pd.DataFrame): Input DataFrame with a 'soud' column.

    Returns:
        pd.DataFrame: Filtered DataFrame with only valid court names.
    """
    is_known = df["soud"].isin(court_lookup.keys())
    filtered = df[is_known].copy().reset_index(drop=True)
    n_removed = (~is_known).sum()
    if n_removed > 0:
        print(
            f"Filtered out {n_removed} decisions with unknown or missing court names."
        )
    return filtered


def filter_out_bad_case_numbers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows with invalid or unparsable 'jednaciCislo' values.

    Uses `parse_jednaciCislo()` to check whether the case number string
    is valid and structured. Only rows that parse successfully are retained.

    Args:
        df (pd.DataFrame): Input DataFrame with a 'jednaciCislo' column.

    Returns:
        pd.DataFrame: Filtered DataFrame with only valid case numbers.
    """
    is_valid = df["jednaciCislo"].apply(parse_jednaciCislo).notna()
    filtered = df[is_valid].copy().reset_index(drop=True)
    n_removed = (~is_valid).sum()
    if n_removed > 0:
        print(f"Filtered out {n_removed} rows with invalid jednaciCislo values.")
    return filtered
