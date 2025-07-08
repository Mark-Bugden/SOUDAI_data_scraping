import threading

from tqdm import tqdm

from config import CHECKPOINT_CSV_PATH, PREPROCESSED_CSV_PATH, RAW_DIR
from scraping.infosoud.utils.checkpointing import (
    deduplicate_checkpoint,
    process_and_update_checkpoint,
    validate_checkpoint,
)
from scraping.infosoud.utils.io import (
    load_done_urls,
    load_or_create_preprocessed,
)
from scraping.infosoud.utils.threading import StopFlag, listen_for_quit
from scraping.infosoud.utils.timeline import get_next_pending_chunk

CHUNK_SIZE = 50


def main():
    # Load the dataframe of all the cases
    df_preprocessed = load_or_create_preprocessed(PREPROCESSED_CSV_PATH, RAW_DIR)
    print("Preprocessed dataframe loaded.")

    # Start the listener thread
    stop_flag = StopFlag()
    listener_thread = threading.Thread(
        target=listen_for_quit, args=(stop_flag,), daemon=True
    )
    listener_thread.start()

    # Load the checkpoint file if it exists
    deduplicate_checkpoint(CHECKPOINT_CSV_PATH)
    validate_checkpoint(df_preprocessed, CHECKPOINT_CSV_PATH)
    done_urls = load_done_urls(CHECKPOINT_CSV_PATH)
    done_count = len(done_urls)

    overall_bar = tqdm(
        total=len(df_preprocessed),
        desc="Total progress",
        position=0,
        initial=done_count,
    )

    chunk_bar = tqdm(total=CHUNK_SIZE, desc="Current chunk", position=1)

    while True:
        done_urls = load_done_urls(CHECKPOINT_CSV_PATH)

        df_chunk = get_next_pending_chunk(df_preprocessed, done_urls, CHUNK_SIZE)

        if df_chunk.empty:
            print("All rows processed. Exiting.")
            break

        chunk_bar.reset(total=len(df_chunk))  # reset for this chunk
        chunk_bar.set_description("Current chunk")

        process_and_update_checkpoint(df_chunk, CHECKPOINT_CSV_PATH, chunk_bar)

        if stop_flag.is_requested():
            print("\nExiting after current chunk due to user request.")
            break

        overall_bar.update(len(df_chunk))

    chunk_bar.close()
    overall_bar.close()


if __name__ == "__main__":
    main()
