import pandas as pd


def remove_outliers(df: pd.DataFrame, years: int = 3):
    """Remove rows which contain outliers in the days_to_decision column."""
    df = df.copy()

    # Define valid range
    max_days = 365 * years
    is_valid = (
        df["days_to_decision"].notna()
        & (df["days_to_decision"] > 0)
        & (df["days_to_decision"] <= max_days)
    )

    return df[is_valid]
