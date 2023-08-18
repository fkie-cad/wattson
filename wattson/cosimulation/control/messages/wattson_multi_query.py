from typing import List

from wattson.cosimulation.control.messages.wattson_query import WattsonQuery


class WattsonMultiQuery(WattsonQuery):
    def __init__(self, queries: List[WattsonQuery]):
        super().__init__()
        self.queries = queries
