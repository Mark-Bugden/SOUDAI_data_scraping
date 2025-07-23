import contextlib
import io

from inputimeout import TimeoutOccurred, inputimeout


class StopFlag:
    """Thread-safe stop signal shared between main thread and input listener."""

    def __init__(self):
        self._stop = False

    def request_stop(self) -> None:
        self._stop = True

    def is_requested(self) -> bool:
        return self._stop


def listen_for_quit(stop_flag, timeout=1):
    """
    Listens for the user to type 'q' to request an early stop.
    Runs quietly without interfering with tqdm output.
    """
    print("Press 'q' and Enter at any time to stop after the current chunk.")
    while not stop_flag.is_requested():
        try:
            # Suppress unwanted output from inputimeout
            with contextlib.redirect_stdout(io.StringIO()):
                user_input = inputimeout(prompt="", timeout=timeout)
            if user_input.strip().lower() == "q":
                stop_flag.request_stop()
                print("\nStop requested by user.")
        except TimeoutOccurred:
            continue
