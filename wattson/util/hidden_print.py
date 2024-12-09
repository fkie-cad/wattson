import os, sys


class HiddenPrint:
    def __init__(self, enabled: bool = True):
        self._enabled = enabled

    def __enter__(self):
        if self._enabled:
            self._original_stdout = sys.stdout
            self._original_stderr = sys.stderr
            sys.stdout = open(os.devnull, 'w')
            sys.stderr = open(os.devnull, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._enabled:
            sys.stdout.close()
            sys.stderr.close()
            sys.stdout = self._original_stdout
            sys.stderr = self._original_stderr
