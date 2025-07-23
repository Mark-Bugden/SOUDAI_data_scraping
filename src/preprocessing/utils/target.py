import pandas as pd


def create_target_variable(df: pd.DataFrame) -> pd.DataFrame:
    """Creates the target variable column."""
    df = df.copy()

    df["days_to_decision"] = (df["date_decision"] - df["date_start"]).dt.days

    return df
