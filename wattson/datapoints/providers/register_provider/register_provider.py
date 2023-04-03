from wattson.datapoints.interface import DataPointProvider, DataPointValue
from wattson.util import get_logger
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from wattson.datapoints.manager import DataPointManager


class RegisterProvider(DataPointProvider):
    def __init__(self, provider_configuration: dict, points: dict, manager: 'DataPointManager'):
        super().__init__(provider_configuration, points, manager)
        self.node_id = self.config["host"]
        self.logger = self.config.get("logger", get_logger(self.node_id, "RegisterProvider"))
        self.registers = {}

    def get_value(self, identifier: str, provider_id: int, disable_cache: bool = False,
                  state_id: Optional[str] = None) -> DataPointValue:
        provider_info = self._get_provider_info(identifier, provider_id, "sources")
        register_name = provider_info["name"]
        default_value = provider_info["default"]
        if register_name in self.registers:
            return self.registers[register_name]
        return default_value

    def set_value(self, identifier: str, provider_id: int, value: DataPointValue) -> bool:
        provider_info = self._get_provider_info(identifier, provider_id, "targets")
        register_name = provider_info["name"]
        self.registers[register_name] = value
        return True

    def start(self):
        pass

    def stop(self):
        self.registers = {}

    def _get_provider_info(self, identifier: str, index: int, key: str = "sources"):
        dp = self.data_points[identifier]
        if key not in dp["providers"]:
            raise ValueError("Invalid provider address")
        provider = dp["providers"][key][index]
        if provider["provider_type"] != "register":
            raise ValueError("Provider address does not represent register provider")
        return provider["provider_data"]
