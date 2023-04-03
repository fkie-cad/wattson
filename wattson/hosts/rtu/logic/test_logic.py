import os
import signal
import subprocess
import threading
import time
from c104 import Cot
from typing import Optional
from typing import Optional

from wattson.datapoints.interface import DataPointValue
from wattson.hosts.rtu.rtu_logic import RTULogic
from wattson.iec104.interface.server import IECServerInterface
from wattson.iec104.interface.types import COT
from wattson.powergrid.common.events import MTU_READY
from wattson.datapoints.providers.pandapower_provider.provider import PandapowerProvider


class TestLogic(RTULogic):
    def __init__(self, rtu: 'RTU', **kwargs):
        super().__init__(rtu, **kwargs)
        self._monitored_dps = {}
        self._delay = 10
        self.config = kwargs
        self.thresholds = {}
        self.switches_dps = {}

    def on_start(self):
        super().on_start()

        # wie komm ich an den switch um ihn auf / zu zu machen?
        all_datapoint_ids = self.rtu.manager.get_data_points()
        for _id in all_datapoint_ids:
            datapoint = self.rtu.manager.data_points[_id]
            for _type in datapoint["providers"].keys():
                for provider in datapoint["providers"][_type]:
                    if provider["provider_type"] == "pandapower":
                        self._monitored_dps[_id] = datapoint
                        self.thresholds.update({datapoint["datatype"]: datapoint["value"]})
                    if provider["provider_data"]["pp_table"] == "switch" and provider["provider_data"]["pp_column"] == "closed":
                        self.switches_dps[_id] = datapoint

        for _type in ["u_under", "u_low", "u_high", "u_over"]:
            if _type in self.config:
                self.thresholds[_type] = self.config[_type]
        self.rtu.manager.add_on_change_callback(callback=self._on_change, ids={dp for dp in self._monitored_dps.keys()})

    def on_stop(self):
        super().on_stop()

    def _on_change(self, identifier: str, value: DataPointValue, state_id: Optional[str]):
        if identifier in self._monitored_dps.keys():
            datapoint = self.rtu.manager.data_points[identifier]
            for provider_type in datapoint["providers"].keys():
                providers = datapoint["providers"][provider_type]
                for provider in providers:
                    provider_data = provider["provider_data"]
                    if provider_data["pp_table"] == "res_bus" and provider_data["pp_column"] == "vm_pu":
                        if value <= self.thresholds["u_low"]:
                            self.logger.info("Low voltage detected.")

                        if value <= self.thresholds["u_under"]:
                            self.logger.info("Under voltage detected.")
                            provider_obj = self.rtu.manager.providers["pandapower"]
                            switch_path_set = provider_obj._identifier_to_path_map[list(self.switches_dps.keys())[0]]  # TODO: Nimm was anderes als einfach nur den ersten
                            self.logger.info(f"{self.switches_dps=}")
                            for path in switch_path_set:
                                table, index, column = path.split(".")
                                index = int(index)
                                response = self.rtu.coord_client.update_value(table=table, column=column, index=index, value=False)
                                # value = self.rtu.coord_client.retrieve_value(table=table, column=column, index=index)
                        if value >= self.thresholds["u_high"]:
                            self.logger.info("High voltage detected.")
                        if value >= self.thresholds["u_over"]:
                            self.logger.info("Over voltage detected.")

        pass

    def configure(self):
        super().configure()
