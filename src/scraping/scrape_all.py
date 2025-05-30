import os
import sys
from datetime import date

from tqdm import tqdm

from scraping.scraping_utils import (
    get_available_years,
    get_days_for_month,
    get_decisions_for_day,
    get_months_for_year,
    make_output_path,
)


def collect_all_available_dates() -> list[date]:
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


def scrape_all() -> None:
    """
    Download and save decision metadata for all available dates.
    Skips already-downloaded pages.
    """
    print("Getting all dates with decisions...")
    all_dates = collect_all_available_dates()

    for date_obj in tqdm(all_dates, desc="Scraping decision data"):
        # Skip if page 1 is already downloaded
        page1_path = make_output_path(date_obj, page=1)
        if os.path.exists(page1_path):
            print(f"[{date_obj}] Already exists — skipping.")
            continue

        try:
            get_decisions_for_day(date_obj)
        except Exception as e:
            print(f"[{date_obj}] Failed to download: {e}")

        # Clear stdout so output isn't cluttered
        sys.stdout.flush()
        print("\033[K", end="")  # ANSI code to clear line


if __name__ == "__main__":
    scrape_all()
