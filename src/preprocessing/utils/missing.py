import pandas as pd


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    return df.dropna()
