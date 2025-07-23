from pathlib import Path

import pandas as pd

from config import IMPORTANT_EVENTS


def load_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-8")

    iso_date_cols = ["datumVydani", "datumZverejneni"]  # e.g. "2024-12-04"
    dmy_date_cols = [
        f"timeline_{col}" for col in list(IMPORTANT_EVENTS)
    ]  # e.g. "30.08.2024"

    for col in iso_date_cols:
        df[col] = pd.to_datetime(df[col], format="%Y-%m-%d", errors="coerce")

    for col in dmy_date_cols:
        df[col] = pd.to_datetime(df[col], format="%d.%m.%Y", errors="coerce")

    return df


def save_csv(df: pd.DataFrame, path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(
        parents=True, exist_ok=True
    )  # Create the directory if it doesn't exist
    df.to_csv(path, index=False, encoding="utf-8")
