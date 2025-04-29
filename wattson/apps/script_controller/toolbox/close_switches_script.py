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
        # Attack strategy type
        self.strategy_type: str = self.config.get("strategy_type", "intermittent")
        # Attack strategy
        self.strategy: list = self.config.get("strategy", [])
        # Elements to exclude
        self.exclude_element_identifiers = self.config.get("exclude", [])

    def run(self):
        if self.wait_for_mtu:
            self.controller.wattson_client.event_wait(MTU_READY_EVENT)
        if self.start_delay > 0 and self.strategy_type == "intermittent":
            self.logger.info(f"Waiting {self.start_delay} seconds before starting the script")
            time.sleep(self.start_delay)
        self.logger.info("CloseSwitchesScript Started")
        self.grid_wrapper.on_element_update(self._on_element_update)
        if self.strategy_type == "intermittent":
            self.logger.info("Attack strategy: intermittent")
            self._open_close_switches()
        elif self.strategy_type == "explicit":
            self.logger.info("Attack strategy: explicit")
            self._setup_explicit_strategy()

    def stop(self):
        self._terminate.set()
        for thread in self._threads.values():
            if thread.is_alive():
                thread.join()

    def _delay_set_data_point(self, coa, ioa, value, delay):
        # Wait for termination or the delay to pass
        if self._terminate.wait(delay):
            return
        
        self.logger.info(f"Sending {coa}.{ioa} = {value} - delay has passed")
        self.controller.set_data_point(coa, ioa, value, block=False)

    def queue_set_data_point(self, coa, ioa, value, delay=None):
        if delay is None:
            delay = self.close_delay
        
        if not delay:
            self.logger.info(f"Sending {coa}.{ioa} = {value}")
            self.controller.set_data_point(coa, ioa, value, block=False)
        else:
            # Queue the data point
            self.logger.info(f"Queueing {coa}.{ioa} = {value}")
            t = threading.Thread(target=self._delay_set_data_point, args=(coa, ioa, value, delay))
            t.start()
            self._threads[id(t)] = t

    def _on_element_update(self, grid_value: GridValue, old_value: Any, value: Any):
        if isinstance(grid_value.get_grid_element(), Switch):
            switch = typing.cast(Switch, grid_value.get_grid_element())
            if self.only_on_change and old_value == value:
                return

            # Modify switch status
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
            switchArr = [f"switch.{swIndex}" for swIndex in self.strategy]
            if self.strategy_type == "intermittent" and switch.get_identifier() in switchArr:
                self.logger.info(f"Detected Switch {switch.get_identifier()} - modifying its state by setting {coa}.{ioa}")
                # Negate the old value
                self.queue_set_data_point(coa, ioa, not value)
                self.logger.info(f"Switch {switch.get_identifier()} set as: {not value}")
            else:
                # For any other switch or for strategy_type explicit do not further alter its state
                self.logger.info(f"Detected Switch {switch.get_identifier()} - no further modifications required {coa}.{ioa}")

    def _setup_explicit_strategy(self):
        # Build a switches dictionary
        switches = self.grid_wrapper.get_grid_elements("switch")
        switchesDict: dict = {}
        for sw in switches:
            switch = typing.cast(Switch, sw)
            switchesDict[switch.get_identifier()] = switch

        for strategyElem in self.strategy:
            switchId = strategyElem["switch_id"]
            time = strategyElem["time"]
            isClosed = strategyElem["is_closed"]
            # Modify switch status
            switchIdentifier = f"switch.{switchId}"
            dp = self._data_point_cache.get(switchIdentifier)
            if dp is None:
                grid_value_setter = switchesDict[switchIdentifier].get_config("closed")
                dps = self.grid_wrapper.get_data_points_for_grid_value(grid_value=grid_value_setter)
                dps = [dp for dp in dps if dp["protocol_data"]["direction"] == "control"]
                if len(dps) != 1:
                    self.logger.error(f"Cannot find DP to open/close switch: {switchIdentifier}")
                    return
                dp = dps[0]
                self._data_point_cache[switchIdentifier] = dp
            info = self.grid_wrapper.get_104_info(dp)
            coa = info["coa"]
            ioa = info["ioa"]
            self.logger.info(f"COA: {coa}, IOA: {ioa}")
            self.logger.info(f"Detected Switch {switchIdentifier} - queuing its state modification by setting {coa}.{ioa}")            
            self.queue_set_data_point(coa, ioa, isClosed, time)
            self.logger.info(f"Switch {switchIdentifier} set as: {isClosed} with delay {time}")
            

    def _open_close_switches(self):
        switches = self.grid_wrapper.get_grid_elements("switch")
        # Just attack the switch with index 4 and 5 (circuit breaker)
        switchArr = [f"switch.{swIndex}" for swIndex in self.strategy]
        for sw in switches:
            switch = typing.cast(Switch, sw)
            self.logger.info(f"Switch {switch.get_identifier()} is_closed: {switch.get_measurement_value('is_closed')}")
            if switch.get_identifier() in switchArr:
                # Modify switch status
                dp = self._data_point_cache.get(switch.get_identifier())
                if dp is None:
                    grid_value_setter = switch.get_config("closed")
                    dps = self.grid_wrapper.get_data_points_for_grid_value(grid_value=grid_value_setter)
                    dps = [dp for dp in dps if dp["protocol_data"]["direction"] == "control"]
                    if len(dps) != 1:
                        self.logger.error(f"Cannot find DP to open/close switch: {sw.get_identifier()}")
                        return
                    dp = dps[0]
                    self._data_point_cache[switch.get_identifier()] = dp
                info = self.grid_wrapper.get_104_info(dp)
                coa = info["coa"]
                ioa = info["ioa"]
                self.logger.info(f"Detected Switch {switch.get_identifier()} - modifying its state by setting {coa}.{ioa}")
                value: bool = switch.get_measurement_value("is_closed")
                if switch.get_identifier() in switchArr:
                    self.queue_set_data_point(coa, ioa, not value)
                    self.logger.info(f"Switch {switch.get_identifier()} set as: {not value}")