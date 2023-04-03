from dataclasses import dataclass
from pathlib import Path


@dataclass
class ProcessInfo:
    pid: int
    host_id: str
    host: dict
    host_config: dict
    deploy_info: dict
    directory: Path
