import threading
import time
import typing

from powerowl.layers.powergrid.elements import Switch
from powerowl.layers.powergrid.values.grid_value import GridValue

from wattson.apps.script_controller.interface import SoloScript

from typing import TYPE_CHECKING, Optional, Dict, Any
from wattson.iec104.common import MTU_READY_EVENT

if TYPE_CHECKING:
    from wattson.apps.script_controller import ScriptControllerApp


# noinspection DuplicatedCode
class CloseSwitchesScript(SoloScript):
    def __init__(self, controller: 'ScriptControllerApp', config: Optional[Dict] = None):
        super().__init__(controller, config)
        self.logger = self.controller.logger.getChild("CloseSwitchesScript")
        self.grid_wrapper = self.controller.grid_wrapper
        self._data_point_cache = {}
        self._active_data_points = set()
        self._threads: Dict[int, threading.Thread] = {}
        self._terminate = threading.Event()
        # Number of seconds to wait before sending the counter command
        self.close_delay = self.config.get("close_delay", 0)
        # Whether to explicitly wait for the MTU to be ready
        self.wait_for_mtu = self.config.get("wait_for_mtu", False)
        # A number of seconds to wait before starting (applies after the potential MTU wait)
        self.start_delay = self.config.get("start_delay", 0)
        # Whether to only change switches that change their state
        self.only_on_change = self.config.get("only_on_change", False)
        # Elements to exclude
        self.exclude_element_identifiers = self.config.get("exclude", [])

    def run(self):
        if self.wait_for_mtu:
            self.controller.wattson_client.event_wait(MTU_READY_EVENT)
        if self.start_delay > 0:
            time.sleep(self.start_delay)
        self.grid_wrapper.on_element_update(self._on_element_update)

    def stop(self):
        self._terminate.set()
        for thread in self._threads.values():
            if thread.is_alive():
                thread.join()

    def _delay_set_data_point(self, coa, ioa, value):
        # Wait for termination or the delay to pass
        if self._terminate.wait(self.close_delay):
            return
        # Data point is no longer active
        try:
            self._active_data_points.remove(f"{coa}.{ioa}")
        finally:
            # Set data point
            self.logger.info(f"Sending {coa}.{ioa} = {value} - delay has passed")
            self.controller.set_data_point(coa, ioa, value, block=False)

    def queue_set_data_point(self, coa, ioa, value):
        if not self.close_delay:
            self.logger.info(f"Sending {coa}.{ioa} = {value}")
            self.controller.set_data_point(coa, ioa, value, block=False)
        else:
            if f"{coa}.{ioa}" in self._active_data_points:
                # Already queued
                self.logger.info(f"Skipping {coa}.{ioa} = {value} - already queued")
                return

            self.logger.info(f"Queueing {coa}.{ioa} = {value}")
            self._active_data_points.add(f"{coa}.{ioa}")
            t = threading.Thread(target=self._delay_set_data_point, args=(coa, ioa, value))
            t.start()
            self._threads[id(t)] = t

    def _on_element_update(self, grid_value: GridValue, old_value: Any, value: Any):
        if isinstance(grid_value.get_grid_element(), Switch):
            switch = typing.cast(Switch, grid_value.get_grid_element())
            if self.only_on_change and old_value == value:
                return
            if not value:
                # Switch is open - close it
                dp = self._data_point_cache.get(switch.get_identifier())
                if dp is None:
                    grid_value_setter = switch.get_config("closed")
                    dps = self.grid_wrapper.get_data_points_for_grid_value(grid_value=grid_value_setter)
                    dps = [dp for dp in dps if dp["protocol_data"]["direction"] == "control"]
                    if len(dps) != 1:
                        self.logger.error(f"Cannot find DP for open switch: {grid_value.get_grid_element().get_identifier()}")
                        return
                    dp = dps[0]
                    self._data_point_cache[switch.get_identifier()] = dp
                info = self.grid_wrapper.get_104_info(dp)
                coa = info["coa"]
                ioa = info["ioa"]
                self.logger.info(f"Detected open Switch {switch.get_identifier()} - closing by setting {coa}.{ioa}")
                self.queue_set_data_point(coa, ioa, True)
            else:
                self.logger.info(f"Switch {switch.get_identifier()} reported as closed")
