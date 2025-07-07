import os
import threading

import pandas as pd
from tqdm import tqdm

from config import INTERIM_DIR, RAW_DIR
from scraping.infosoud.utils import (
    StopFlag,
    add_infosoud_urls,
    add_parsed_jednaciCislo,
    deduplicate_checkpoint,
    extract_case_timeline,
    filter_out_bad_decisions,
    listen_for_quit,
    load_all_decisions,
    load_done_urls,
    validate_checkpoint,
)

# Checkpoint path
preprocessed_csv_path = INTERIM_DIR / "preprocessed_decisions.csv"
checkpoint_csv_path = INTERIM_DIR / "infosoud_checkpoint.csv"
CHUNK_SIZE = 50


# If the preprocessed_decisions.csv file does not exist, create it
if not os.path.exists(preprocessed_csv_path):
    df_raw_json = load_all_decisions(base_path=RAW_DIR)
    df_filtered = filter_out_bad_decisions(df_raw_json)
    df_parsed = add_parsed_jednaciCislo(df_filtered)
    df_preprocessed = add_infosoud_urls(df_parsed)
    df_preprocessed.to_csv(preprocessed_csv_path, index=False)
    print("Preprocessed dataframe created and saved.")

# Load the preprocessed_decisions.csv file
df_preprocessed = pd.read_csv(preprocessed_csv_path)
print("Preprocessed dataframe loaded.")

# Start the listener thread
stop_flag = StopFlag()
listener_thread = threading.Thread(
    target=listen_for_quit, args=(stop_flag,), daemon=True
)
listener_thread.start()


# Load the checkpoint file if it exists
deduplicate_checkpoint(checkpoint_csv_path)
validate_checkpoint(df_preprocessed, checkpoint_csv_path)
done_urls = load_done_urls(checkpoint_csv_path)
done_count = len(done_urls)

overall_bar = tqdm(
    total=len(df_preprocessed), desc="Total progress", position=0, initial=done_count
)

chunk_bar = tqdm(total=CHUNK_SIZE, desc="Current chunk", position=1)

while True:
    done_urls = load_done_urls(checkpoint_csv_path)  # always refresh

    # Get only rows not yet processed
    df_pending = df_preprocessed[~df_preprocessed["infosoud_url"].isin(done_urls)]

    if df_pending.empty:
        print("All rows processed. Exiting.")
        break

    # Get the next chunk
    df_chunk = df_pending.head(CHUNK_SIZE)
    chunk_length = len(df_chunk)

    chunk_bar.reset(total=chunk_length)  # reset for this chunk
    chunk_bar.set_description("Current chunk")

    new_rows = []

    for indx, row in df_chunk.iterrows():
        try:
            timeline = extract_case_timeline(row["infosoud_url"])
        except Exception as e:
            timeline = {
                k: None
                for k in [
                    "Zahájení řízení",
                    "Nařízení jednání",
                    "Vydání rozhodnutí",
                    "Vyřízení věci",
                    "Datum pravomocného ukončení věci",
                ]
            }
            print(f"Error encountered in row index {indx}. Error: {e}")
        new_row = row.to_dict()
        new_row.update(timeline)
        new_rows.append(new_row)

        chunk_bar.update(1)
        overall_bar.update(1)

    # Convert to DataFrame
    df_new = pd.DataFrame(new_rows)

    # Merge with existing checkpoint and deduplicate
    if os.path.exists(checkpoint_csv_path):
        df_checkpoint = pd.read_csv(checkpoint_csv_path, dtype=str)
        df_combined = pd.concat([df_checkpoint, df_new], ignore_index=True)
        df_combined = df_combined.drop_duplicates(subset="infosoud_url")
        df_combined.to_csv(checkpoint_csv_path, index=False)
    else:
        df_new.to_csv(checkpoint_csv_path, index=False)

    if stop_flag.is_requested():
        print("Exiting after current chunk due to user request.")
        break

chunk_bar.close()
overall_bar.close()
