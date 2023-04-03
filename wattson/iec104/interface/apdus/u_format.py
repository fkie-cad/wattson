from dataclasses import dataclass
from typing import Any


@dataclass
class U_FORMAT:
    other_info: Any

    def __str__(self):
        return f"U-Format({self.other_info})"
