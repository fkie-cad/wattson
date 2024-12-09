from typing import Optional, Any

from wattson.cosimulation.control.messages.wattson_response import WattsonResponse


class WattsonQuery:
    def __init__(self, query_type: Optional[str] = None, query_data: Any = None):
        self._handled: int = 0
        self.query_type: Optional[str] = query_type
        self.query_data: Any = query_data
        if self.query_data is None:
            self.query_data = {}
        self.allow_multi_handling = False
        self.response: Optional[WattsonResponse] = None
        self.client_id: Optional[str] = None

    def __repr__(self):
        return f"{self.query_type} // {repr(self.query_data)}"

    def requires_native_namespace(self) -> bool:
        return True

    def add_response(self, response: WattsonResponse):
        self.response = response

    def mark_as_handled(self):
        self._handled += 1

    def is_handled(self):
        return self._handled >= 1

    def can_be_handled(self):
        if self.allow_multi_handling:
            return True
        return not self.is_handled()

    def has_response(self) -> bool:
        return self.response is not None

    def has_successful_response(self) -> bool:
        return self.has_response() and self.response.is_successful()
