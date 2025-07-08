from tqdm import tqdm


class StopFlag:
    """Thread-safe stop signal shared between main thread and input listener."""

    def __init__(self):
        self._stop = False

    def request_stop(self) -> None:
        self._stop = True

    def is_requested(self) -> bool:
        return self._stop


def listen_for_quit(stop_flag: StopFlag) -> None:
    """
    Listens for the user to type 'q' and press Enter. If detected,
    sets the stop flag to request graceful shutdown after current chunk.
    """
    while True:
        user_input = (
            input("Type 'q' and press Enter to stop after current chunk: \n")
            .strip()
            .lower()
        )
        if user_input == "q":
            tqdm.write("Stop requested. Will exit after current chunk.")
            stop_flag.request_stop()
            break
