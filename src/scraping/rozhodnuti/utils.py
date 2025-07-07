import json
import os
from datetime import date, datetime

import requests

# This is the base URL from which we obtain the court decisions
BASE_URL = "https://rozhodnuti.justice.cz/api/opendata"

#


def fetch_json(url: str) -> dict:
    """
    Fetch and return parsed JSON from the given URL.

    Args:
        url (str): The API endpoint to fetch.

    Returns:
        dict: The parsed JSON response.
    """

    headers = {"User-Agent": "SOUDAI-scraper/1.0"}
    response = requests.get(url, headers=headers, timeout=60)
    response.raise_for_status()

    return response.json()


def get_available_years() -> list[int]:
    """
    Retrieve the list of years that contain published decisions.

    Returns:
        list[int]: A list of available years.
    """

    data = fetch_json(BASE_URL)

    return [entry["rok"] for entry in data]


def get_months_for_year(year: int) -> list[int]:
    """
    Retrieve the list of months with decisions in a given year.

    Args:
        year (int): The year to query.

    Returns:
        list[int]: A list of available month numbers (1-12) for the given year.
    """

    url = f"{BASE_URL}/{year}"
    data = fetch_json(url)

    return [entry["mesic"] for entry in data]


def get_days_for_month(year: int, month: int) -> list[date]:
    """
    Retrieve all available decision dates in a given year and month.

    Args:
        year (int): Year to query.
        month (int): Month to query.

    Returns:
        list[date]: A list of date objects representing each day with decisions in the
        given year/month.
    """

    url = f"{BASE_URL}/{year}/{month:02d}"
    data = fetch_json(url)

    return [datetime.strptime(entry["datum"], "%Y-%m-%d").date() for entry in data]


def get_all_available_dates() -> list[date]:
    """
    Traverse the API hierarchy and return a list of all available dates
    with court decisions.

    Returns:
        list[date]: All available court decision dates.
    """

    all_dates = []
    years = get_available_years()

    for year in years:
        try:
            months = get_months_for_year(year)
        except Exception as e:
            print(f"[{year}] Failed to get months: {e}")
            continue

        for month in months:
            try:
                days = get_days_for_month(year, month)
                all_dates.extend(days)
            except Exception as e:
                print(f"[{year}-{month:02d}] Failed to get days: {e}")
                continue

    return all_dates


def get_decisions_for_day(date_obj: date) -> None:
    """
    Download and save all paginated decision data for a given day,
    starting at page 0 (as required by the API).
    """
    page = 0
    total_pages = None

    while True:
        url = (
            f"{BASE_URL}/{date_obj.year}/"
            f"{date_obj.month:02d}/{date_obj.day:02d}"
            f"?page={page}"
        )
        data = fetch_json(url)

        if "items" not in data or not isinstance(data["items"], list):
            print(f"[{date_obj}] Unexpected format on page {page}, stopping.")
            break

        if total_pages is None:
            total_pages = data.get("totalPages", 1)

        if page >= total_pages:
            break

        if not data["items"]:
            print(f"[{date_obj}] Page {page} has no items, stopping early.")
            break

        path = make_output_path(date_obj, page)
        save_json(data, path)

        page += 1


def save_json(data: dict, path: str) -> None:
    """
    Save a JSON-serializable object to a file.

    Args:
        data (dict): The data to save.
        path (str): The destination file path.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def make_output_path(date_obj: date, page: int) -> str:
    """
    Generate the output file path for a given date and page number.

    Args:
        date_obj (date): The date of the decisions.
        page (int): The page number (starting from 0).

    Returns:
        str: The file path to store the JSON data.
    """
    return (
        f"data/raw/{date_obj.year}/{date_obj.month:02d}/"
        f"{date_obj.day:02d}/page{page}.json"
    )
