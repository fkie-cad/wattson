from typing import Callable
import threading


class AsyncResolve:
    @staticmethod
    def resolve(callback: Callable, *args, **kwargs):
        t = threading.Thread(target=AsyncResolve._call_callback, args=(callback, args, kwargs))
        t.daemon = True
        t.start()
        return t

    @staticmethod
    def _call_callback(callback: Callable, args, kwargs):
        callback(*args, **kwargs)
