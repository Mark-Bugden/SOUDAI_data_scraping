import os
import sys

from tqdm import tqdm

from scraping.rozhodnuti.utils import (
    get_all_available_dates,
    get_decisions_for_day,
    make_output_path,
)


def scrape_all() -> None:
    """
    Download and save decision data for all available dates.
    Skips already-downloaded pages.
    """
    print("Getting all dates with decisions...")
    all_dates = get_all_available_dates()

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
