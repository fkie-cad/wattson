import dataclasses
from typing import Optional

from wattson.cosimulation.simulators.network.constants import DEFAULT_SEGMENT


@dataclasses.dataclass(kw_only=True)
class WattsonSegment:
    name: str
    server_port: Optional[int] = None

    def is_main_segment(self) -> bool:
        return self.name == DEFAULT_SEGMENT
