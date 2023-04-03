from dataclasses import dataclass
from typing import Any


@dataclass
class S_FORMAT:
    other_info: Any

    def __str__(self):
        return f"S-Format({self.other_info})"
