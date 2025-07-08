from typing import Optional

import pandas as pd


def parse_jednaciCislo(case_number: str) -> Optional[list]:
    """
    Parse a Czech court case identifier (jednací číslo) into its structured components.

    Expected format is: "<senát> <druh věci> <pořadové číslo>/<ročník>[-číslo odvolání]"
    for example: "12 C 123/2020-15".

    Args:
        case_number (str): The jednací číslo string to parse.

    Returns:
        Optional[list]: A list containing the parsed components:
            [
                senát (int),
                druh věci (str),
                pořadové číslo (int),
                ročník (int),
                číslo odvolání (int or None)
            ]

            or None if parsing fails.
    """

    if not isinstance(case_number, str):
        return None
    try:
        parts = case_number.split(" ")
        if len(parts) != 3:
            return None
        cislo_senatu = int(parts[0])
        druh_vec = parts[1]
        third = parts[2]
        if "-" in third:
            slash_part, dash = third.split("-")
            dash_num = int(dash)
        else:
            slash_part = third
            dash_num = None
        bc_vec, rocnik = slash_part.split("/")
        bc_vec = int(bc_vec)
        rocnik = int(rocnik)
        return [cislo_senatu, druh_vec, bc_vec, rocnik, dash_num]
    except Exception:
        return None


def add_parsed_jednaciCislo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a parsed version of 'jednaciCislo' as a new column.

    Returns a copy of the input DataFrame with a 'parsed_jednaciCislo' column.
    """

    df = df.copy()
    df["parsed_jednaciCislo"] = df["jednaciCislo"].apply(parse_jednaciCislo)
    return df
    df["parsed_jednaciCislo"] = df["jednaciCislo"].apply(parse_jednaciCislo)
    return df
