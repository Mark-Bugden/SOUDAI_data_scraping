import pandas as pd


def clean_date_data(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans the date data.

    The three relevant dates for us are the

    - Start date ("timeline_Zahájení řízení"),
    - Date of decision ("datumVydani" ~ "timeline_Vydání rozhodnutí")
    - End date ("timeline_Datum pravomocného ukončení věci")

    the last is otherwise known as the Legal force of the final decision.
    This function ensures that decision date data from different sources matches,
    and then renames the relevant date columns for convenience, and drops the
    irrelevant date columns.

    """
    # Create a working copy
    df = df.copy()

    # Remove rows where the decision date information does not match
    df = remove_conflicting_data(
        df, col1="datumVydani", col2="timeline_Vydání rozhodnutí"
    )

    # Drop the, now redundant. datumVydani column
    print("Dropping redundant column 'datumVydani'")
    df = df.drop("datumVydani", axis=1)

    # Rename columns for easier use
    print("Renaming date columns for convenience.")
    df = df.rename(
        columns={
            "timeline_Zahájení řízení": "date_start",
            "timeline_Vydání rozhodnutí": "date_decision",
            "timeline_Datum pravomocného ukončení věci": "date_end",
        }
    )

    # Drop irrelevant datetime columns
    print("Dropping irrelevant date columns.")
    df = df.drop(
        [
            "datumZverejneni",
            "timeline_Nařízení jednání",
            "timeline_Vyřízení věci",
            "timeline_Skončení věci",
        ],
        axis=1,
    )

    df = remove_invalid_dates(df, date_cols=["date_start", "date_decision", "date_end"])

    return df


def clean_remaining_data(df: pd.DataFrame) -> pd.DataFrame:
    """Removes redundant columns and renames relevant columns for convenience."""
    # Create a working copy
    df = df.copy()

    # Drop irrelevant columns
    print("Dropping irrelevant columns.")
    df = df.drop(
        [
            "jednaciCislo",
            "ecli",
            "odkaz",
            "infosoud_url",
        ],
        axis=1,
    )

    return df


def remove_conflicting_data(df: pd.DataFrame, col1: str, col2: str) -> pd.DataFrame:
    """
    Return a new DataFrame with rows removed where col1 and col2 are not equal.

    Args:
        df: The input DataFrame.
        col1: Name of the first column to compare.
        col2: Name of the second column to compare.

    Returns:
        A new DataFrame where col1 == col2 for all rows.
    """
    # Validate inputs
    if col1 not in df.columns:
        raise ValueError(f"Column '{col1}' not found in DataFrame.")
    if col2 not in df.columns:
        raise ValueError(f"Column '{col2}' not found in DataFrame.")

    # Create a working copy
    df = df.copy()

    # Build mask and filter
    match_mask = df[col1] == df[col2]
    removed_count = (~match_mask).sum()

    if removed_count > 0:
        print(
            f"[remove_conflicting_data] Removed {removed_count} rows "
            f"where '{col1}' != '{col2}'."
        )

    return df[match_mask].reset_index(drop=True)


def remove_invalid_dates(
    df: pd.DataFrame,
    date_cols: list[str],
    min_date: str = "1990-01-01",
    max_date: str = "2100-01-01",
) -> pd.DataFrame:
    """Remove rows with obviously invalid dates in any of the specified columns."""

    df = df.copy()

    min_ts = pd.Timestamp(min_date)
    max_ts = pd.Timestamp(max_date)

    for col in date_cols:
        df = df[df[col].notna() & (df[col] >= min_ts) & (df[col] <= max_ts)]

    return df
