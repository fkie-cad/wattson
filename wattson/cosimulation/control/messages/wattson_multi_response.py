from typing import List, Optional

from wattson.cosimulation.control.messages.wattson_response import WattsonResponse


class WattsonMultiResponse(WattsonResponse):
    def __init__(self, responses: Optional[List[WattsonResponse]] = None):
        super().__init__()
        self.responses = responses if responses is not None else []

    def add_response(self, response: WattsonResponse):
        self.responses.append(response)
