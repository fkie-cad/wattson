import os
import threading
from typing import Optional


class ProgressPrinter:
    """Allows to print a progress bar and indicator on the same line in the terminal."""
    def __init__(self,
                 max_progress: int,
                 current_progress: int = 0,
                 show_bar: bool = True,
                 show_total: bool = True,
                 show_current: bool = True,
                 enable_print: bool = True,
                 auto_stop: bool = True,
                 stop_event: Optional[threading.Event] = None,
                 on_start_margin: bool = True,
                 on_stop_margin: bool = False,
                 show_custom_prefix: bool = False):

        self._lock = threading.RLock()
        self.enable_print: bool = enable_print
        self._started = threading.Event()
        self.max_progress: int = max_progress
        self.current_progress: int = current_progress
        self.show_bar: bool = show_bar
        self.show_total: bool = show_total
        self.show_current: bool = show_current
        self.show_custom_prefix: bool = show_custom_prefix
        self.auto_stop: bool = auto_stop
        self.custom_prefix = ""
        self.stop_event: Optional[threading.Event] = stop_event
        self._on_stop_margin: bool = on_stop_margin
        self._on_start_margin: bool = on_start_margin
        if self.show_total and not self.show_current:
            raise AttributeError("Cannot show total number when not showing current number")

    def start(self):
        if not self.enable_print:
            return
        if self._started.is_set():
            return
        self._started.set()
        if self._on_start_margin:
            print("")
        self.update()

    def stop(self, additional_newline: Optional[bool] = None):
        if additional_newline is not None:
            self._on_stop_margin = additional_newline
        if not self._started.is_set():
            return
        self._started.clear()
        if self.stop_event is not None:
            self.stop_event.set()
        if not self.enable_print:
            return
        print("")
        if self._on_stop_margin:
            print("", flush=True)

    def update(self):
        if not self.enable_print or not self._started.is_set():
            return

        line_prefix = "\r"
        line_end = ""

        with self._lock:
            try:
                term_width = os.get_terminal_size().columns
            except OSError:
                # Output is not in terminal (i.e., redirected to a file)
                line_prefix = ""
                line_end = "\n"
                term_width = 80
            bar_width = term_width - 2
            bar_perc = 1 if self.max_progress == 0 else self.current_progress / self.max_progress
            status_string = ""
            if self.show_current:
                number_len = len(str(self.max_progress))
                if self.show_total:
                    status_string = f"{str(self.current_progress).rjust(number_len)} / {str(self.max_progress)}"
                else:
                    status_string = f"{str(self.current_progress).rjust(number_len)}"

            if self.show_custom_prefix:
                status_string = f"{self.custom_prefix} {status_string}".strip()

            bar_width -= max(len(status_string) + 1, 0)
            bar_fill = min(int(bar_perc * bar_width), bar_width)
            bar_gap = bar_width - bar_fill
            if self.show_current:
                if self.show_bar:
                    print(f"{line_prefix}{status_string} [{'=' * bar_fill}{' ' * bar_gap}]", flush=True, end=line_end)
                else:
                    print(f"{line_prefix}{status_string}", flush=True, end=line_end)
            elif self.show_bar:
                print(f"{line_prefix}[{'=' * bar_fill}{' ' * bar_gap}]", flush=True, end=line_end)

            if self.auto_stop and self.current_progress >= self.max_progress:
                self.stop()

    def inc(self, custom_prefix: Optional[str] = None):
        with self._lock:
            if custom_prefix is not None:
                self.custom_prefix = custom_prefix
            self.current_progress += 1
            self.update()

    def set_progress(self, progress: int, custom_prefix: Optional[str] = None):
        with self._lock:
            if custom_prefix is not None:
                self.custom_prefix = custom_prefix
            self.current_progress = progress
            self.update()

    def set_custom_prefix(self, custom_prefix: str):
        with self._lock:
            self.custom_prefix = custom_prefix
            self.update()
