from typing import Optional

from wattson.analysis.statistics.client.statistic_client import StatisticClient
from wattson.datapoints.interface.data_point_provider import DataPointProvider
from wattson.datapoints.interface.data_point_provider import DataPointValue
from wattson.util import get_logger
from wattson.hosts.rtu_modbus.modbus_client import MODBUS_Client_Maintainer
import os
import time


class ModbusProvider(DataPointProvider):
    def __init__(
        self, provider_configuration: dict, points: dict, manager: "DataPointManager"
    ):
        self.statistics_config = provider_configuration.get("statistics_config", None)
        super().__init__(provider_configuration, points, manager)
        self.cache: Dict[Tuple[str, int], Dict] = {}
        self.cache_decay = self.config.get("cache_decay", 1)

        self.node_id = self.config["host"]
        self.logger = self.config.get(
            "logger", get_logger(self.node_id, "Modbus Provider")
        )

        self.statistics = StatisticClient(
            ip=self.statistics_config.get("server"),
            host=f"modbus_provider_{self.node_id}",
            logger=self.logger,
        )
        self.statistics.start()

        self.source_providers = {}
        for identifier, dp in self.data_points.items():
            if "source" in dp["providers"]:
                for i, provider in enumerate(dp["providers"]["source"]):
                    if provider["provider_type"] == "MODBUS":
                        self._register_source_provider(identifier, i)

    # def _init_modbus_backend(self):
    #     print(self.statistics_config)
    #     return MODBUS_Client_Maintainer(self.fields,
    #                                     statistics_config=self.statistics_config)

    def get_data_points(self):
        return self.data_point_objects

    def get_value(self, identifier: str, provider_id: int, disable_cache: bool = False,
                  state_id: Optional[str] = None) -> DataPointValue:
        point = self.manager.data_points[identifier]
        [unit_id, field_id, address, type_id] = self.get_provider_dat(point)

        time1 = time.time()
        res = self.field_client_maintainer.read_value(
            field_id, address, unit_id, type_id
        )
        time2 = time.time()

        self.statistics.log(
            field_id, event_class="modbus.provider", value=time2-time1
        )

        self.logger.info(f"Modbus provider got {res} of type {type(res)}")
        return res

    def set_value(
        self, identifier: str, provider_id: int, value: DataPointValue
    ) -> bool:
        point = self.manager.data_points[identifier]
        [unit_id, field_id, address, type_id] = self.get_provider_dat(point)

        self.logger.info(f"Modbus provider set {value} of type {type(value)}")

        # TODO check return value
        res = self.field_client_maintainer.write_value(
            field_id, address, unit_id, type_id, value
        )
        return True

    def get_provider_dat(self, point):
        if point["protocol_data"]["direction"] == "monitoring":
            key = "sources"
        else:
            key = "targets"

        provider_dat = point["providers"][key][0]["provider_data"]

        unit_id = provider_dat["unit_id"]
        field_id = provider_dat["field_id"]
        address = provider_dat["address"]
        type_id = provider_dat["type_id"]

        return (unit_id, field_id, address, type_id)

    def start(self):
        self.field_client_maintainer = MODBUS_Client_Maintainer(
            self.config["field_devices"],
            statistics_config=self.statistics_config,
            host_name=self.node_id,
            logger=self.logger,
        )

    def stop(self):
        self.field_client_maintainer.stop()

    def split_identifier(self, identifier):
        return identifier.split(".")
