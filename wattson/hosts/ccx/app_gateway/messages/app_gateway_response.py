from typing import Any


class AppGatewayResponse:
    def __init__(self, successful: bool, data: Any = None):
        self.successful = successful
        self.data = data
        if data is None:
            self.data = {}

    def is_successful(self) -> bool:
        return self.successful

    def is_promise(self) -> bool:
        return False
