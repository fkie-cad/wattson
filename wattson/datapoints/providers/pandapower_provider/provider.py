import threading

from wattson.datapoints.interface import DataPointValue
from wattson.datapoints.interface.data_point_provider import DataPointProvider
from wattson.powergrid import CoordinationClient
from wattson.powergrid.common.events import ELEMENT_UPDATED
from wattson.powergrid.messages import ControlMessageType
from wattson.powergrid.messages.request_response_message import RequestResponseMessage
from wattson.util import get_logger
from time import time
from typing import Dict, Tuple, TYPE_CHECKING, Optional, Callable, List, Set

if TYPE_CHECKING:
    from wattson.datapoints.manager import DataPointManager


class PandapowerProvider(DataPointProvider):
    def __init__(self, provider_configuration: dict, points: dict, manager: 'DataPointManager'):
        super().__init__(provider_configuration, points, manager)
        self.cache: Dict[Tuple[str, int], Dict] = {}
        self.cache_decay = self.config.get("cache_decay", 1)
        self._path_to_identifier_map = {}
        self._identifier_to_path_map = {}
        self._state_lock = threading.Lock()
        self._states = {}
        self._state_id = 0
        self._max_state_count = 2000
        self.filter_paths = set()

        self.node_id = self.config["host"]
        self.logger = self.config.get("logger", get_logger(self.node_id, "Pandapower Provider"))
        self.coord_ip = self.config["coordinator_ip"]
        self.statistics = self.config.get("statistics", None)

        self.source_providers = {}
        for identifier, dp in self.data_points.items():
            # Build Path -> Identifier Map
            self._add_identifier_to_map(identifier, dp)

            if "source" in dp["providers"]:
                for i, provider in enumerate(dp["providers"]["source"]):
                    if provider["provider_type"] == "pandapower":
                        self._register_source_provider(identifier, i)

        self._sim_start_time = None
        self.client = CoordinationClient(
            self.coord_ip,
            node_id=self.node_id,
            logger=self.logger.getChild("CoordinationClient"),
            global_event_handler=self._global_event_handler,
            statistics=self.statistics
        )
        self.client.subscribe(self._on_change, ELEMENT_UPDATED)

    def add_filter_ids(self, ids):
        super().add_filter_ids(ids)
        elements = set()
        for id in ids:
            elements.update(self._identifier_to_path_map.get(id, set()))
        self.client.get_response(RequestResponseMessage({
            "type": "SUBSCRIBE_ELEMENT_UPDATE",
            "elements": elements
        }))

    def add_filter_paths(self, paths: Set):
        """
        Add a callback by specifying the element to change (e.g., res_bus.1.vm_pu)
        :param paths: A set of paths to monitor
        :return:
        """
        self.filter_paths.update(paths)
        self.client.get_response(RequestResponseMessage({
            "type": "SUBSCRIBE_ELEMENT_UPDATE",
            "elements": paths
        }))

    @property
    def _next_state_id(self) -> str:
        self._state_id += 1
        return str(self._state_id)

    def _store_state(self, value) -> str:
        with self._state_lock:
            next_id = self._next_state_id
            self._states[next_id] = value
            to_delete = str(int(self._next_state_id) - self._max_state_count)
            if to_delete in self._states:
                del self._states[to_delete]
            return next_id

    def start(self):
        self._wait_until_ready()

    def get_sim_start_time(self) -> Optional[float]:
        return self._sim_start_time

    def _wait_until_ready(self):
        self.logger.info("Power Simulation Client starting")
        self.client.start()
        self.logger.info("Power Simulation Client started")
        t0 = time()
        if self.client.check_connection():
            self.logger.info(f"Registration at Coordinator successful replied after {time() - t0:.2f} s")
        else:
            msg = f"RTU backend could not query data from Coordinator!"
            self.logger.critical(msg)
            raise TimeoutError(msg)
        self.client.wait_for_start_event()
        self._sim_start_time = self.client.get_sim_start_time()
        self.logger.info(f"Got Simulation start time: {self._sim_start_time}")

    def _on_change(self, _: str, data: dict):
        if self.callbacks is not None:
            table = data["table"]
            column = data["column"]
            index = data["index"]
            value = data["value"]
            state_id = self._store_state(value)
            path = f"{table}.{index}.{column}"
            identifiers = self._get_identifiers_by_path(path)
            for identifier in identifiers:
                for callback in self.callbacks:
                    callback(identifier, value, state_id, "DP_ID")
            if path in self.filter_paths:
                for callback in self.callbacks:
                    callback(path, value, state_id, "PATH")

    def stop(self):
        self.client.stop()

    def set_value(self, identifier: str, provider_id: int, value: DataPointValue) -> bool:
        info = self._get_pp_info(identifier, provider_id, "targets")
        response = self.client.update_value(
            info["pp_table"],
            info["pp_column"],
            info["pp_index"],
            value
        )
        # TODO: Verify this behavior!
        if response is None:
            return False
        return True

    def get_value(self, identifier: str, provider_id: int, disable_cache: bool = False,
                  state_id: Optional[str] = None) -> DataPointValue:
        if state_id is not None:
            if state_id in self._states:
                return self._states[state_id]
            self.logger.debug(f"Requested state {state_id} is unknown")

        if not disable_cache and (identifier, provider_id) in self.cache:
            cache_entry = self.cache[(identifier, provider_id)]
            if cache_entry["time"] > time() - self.cache_decay:
                # Use cached value
                return cache_entry["value"]
        # No cached entry or cache is decayed
        info = self._get_pp_info(identifier, provider_id, "sources")
        try:
            value = self.client.retrieve_value(info["pp_table"], info["pp_column"], info["pp_index"])
        except Exception as e:
            self.logger.error(f"Failed to read pandapower data: {e}")
            return None
        # Update cache
        self.cache[(identifier, provider_id)] = {
            "time": time(),
            "value": value
        }
        return value

    def clear_cache(self):
        self.cache = {}

    def update_cache(self):
        self.cache = {}
        for identifier, providers in self.source_providers.items():
            for index in providers:
                self.get_value(identifier, index)

    def _add_identifier_to_map(self, identifier: str, data_point: dict):
        """
        Get all pandapower paths (table.index.column) for this data point and map them to the identifier
        :param identifier:
        :param data_point:
        :return:
        """
        paths = set()
        dp = data_point
        for p_type in ["sources", "targets"]:
            if p_type in dp["providers"]:
                for provider in dp["providers"][p_type]:
                    if provider["provider_type"] == "pandapower":
                        table = provider["provider_data"]["pp_table"]
                        column = provider["provider_data"]["pp_column"]
                        index = provider["provider_data"]["pp_index"]
                        path = f"{table}.{index}.{column}"
                        paths.add(path)
        self._identifier_to_path_map[identifier] = paths

        for path in paths:
            self._path_to_identifier_map.setdefault(path, set()).add(identifier)

    def _get_identifiers_by_path(self, path: str) -> List[str]:
        return self._path_to_identifier_map.get(path, set())

    def _get_pp_info(self, identifier: str, index: int, key: str = "sources"):
        dp = self.data_points[identifier]
        if key not in dp["providers"]:
            raise ValueError("Invalid provider address")
        provider = dp["providers"][key][index]
        if provider["provider_type"] != "pandapower":
            raise ValueError("Provider address does not represent pandapower provider")
        return provider["provider_data"]

    def _global_event_handler(self, control_message: ControlMessageType) -> None:
        if control_message == ControlMessageType.start:
            self.client.start_event.set()
            self.logger.debug("Global Start Event received")
        elif control_message == ControlMessageType.update:
            self.update_cache()
            self.logger.debug("Power flow update notification received")

    def _register_source_provider(self, identifier, index):
        if identifier not in self.source_providers:
            self.source_providers[identifier] = []
        self.source_providers[identifier].append(index)
