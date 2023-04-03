from wattson.datapoints.interface import DataPointValue
from typing import Optional


class SnippetParser:
    def __init__(self):
        pass

    def parse(self, snippet: Optional[str], namespace: Optional[dict]) -> DataPointValue:
        if snippet is None or snippet == "":
            if namespace is None:
                return 0
            if "V" in namespace:
                return namespace["V"]
            if "X1" in namespace:
                return namespace["X1"]
            return 0
        exec(snippet, {}, namespace)
        if "res" in namespace:
            return namespace["res"]
        return 0
