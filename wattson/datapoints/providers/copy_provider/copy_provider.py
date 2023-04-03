from wattson.datapoints.interface import DataPointProvider, DataPointValue
from wattson.util import get_logger
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from wattson.datapoints.manager import DataPointManager


class CopyProvider(DataPointProvider):
    def __init__(self, provider_configuration: dict, points: dict, manager: 'DataPointManager'):
        super().__init__(provider_configuration, points, manager)
        self.node_id = self.config["host"]
        self.logger = self.config.get("logger", get_logger(self.node_id, "CopyProvider"))

    def get_value(self, identifier: str, provider_id: int, disable_cache: bool = False,
                  state_id: Optional[str] = None) -> DataPointValue:
        provider_info = self._get_provider_info(identifier, provider_id, "sources")
        copy_from = provider_info["identifier"]
        return self.manager.get_value(copy_from)

    def set_value(self, identifier: str, provider_id: int, value: DataPointValue) -> bool:
        provider_info = self._get_provider_info(identifier, provider_id, "targets")
        copy_to = provider_info["identifier"]
        return self.manager.set_value(copy_to, value)

    def start(self):
        pass

    def stop(self):
        pass

    def _get_provider_info(self, identifier: str, index: int, key: str = "sources"):
        dp = self.data_points[identifier]
        if key not in dp["providers"]:
            raise ValueError("Invalid provider address")
        provider = dp["providers"][key][index]
        if provider["provider_type"] != "copy":
            raise ValueError("Provider address does not represent register provider")
        return provider["provider_data"]
