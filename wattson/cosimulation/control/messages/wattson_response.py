from typing import Any, Callable, Optional


class WattsonResponse:
    def __init__(self, successful: bool = False, data: Any = None):
        self._success = successful
        self.data = data
        # Can store a callback that will be called after the response has been sent.
        self._post_send_callback: Optional[Callable] = None
        if data is None:
            self.data = {}

    def is_successful(self) -> bool:
        return self._success

    def set_successful(self, success: bool = True):
        self._success = success

    def is_promise(self) -> bool:
        return False

    def add_post_send_callback(self, callback: Callable):
        self._post_send_callback = callback

    def clear_post_send_callback(self):
        self._post_send_callback = None

    def get_post_send_callback(self) -> Optional[Callable]:
        return self._post_send_callback
