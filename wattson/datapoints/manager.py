import datetime
import threading
import time
from logging import Logger
from pathlib import Path
from typing import List, Type, Optional, Dict, Callable, Set
import importlib

import pytz

from wattson.datapoints.interface import DataPointValue
from wattson.datapoints.data_point import DataPoint
from wattson.datapoints.interface import DataPointProvider
from wattson.datapoints.interface.snippet_parser import SnippetParser
from wattson.iec104.interface.types import COT
from wattson.util import get_logger


class DataPointManager:
    def __init__(self, host: str, points: Dict, provider_conf: Optional[dict] = None, logger: Optional[Logger] = None):
        # Data points as dict / map
        self.data_points = points
        self._written_values = []
        self._sanitize_datapoints()
        # Data point objects as dict / map
        self.data_point_objects = {p["identifier"]: DataPoint(p["identifier"], self) for p in points.values()}

        # Monitoring
        self._on_change_callback_id = 0
        self._on_change_callbacks = {}

        self._allow_reads = threading.Event()
        self._allow_reads.set()

        self.provider_conf = provider_conf if provider_conf is not None else {}
        self.host = host
        self.providers = {}
        self.s_parser = SnippetParser()
        if logger:
            self.logger = logger.getChild("DataPointManager")
        else:
            self.logger = get_logger("DataPointManager", f"DataPointManager at {host}")
        self._init_providers()
        self.logger.info(f"Initialized DataPointManager on host {self.host}")

    def get_data_points(self):
        return self.data_point_objects

    def get_data_point(self, identifier: str) -> Optional[DataPoint]:
        return self.data_point_objects.get(identifier)

    def get_data_point_dict(self, identifier: str) -> Optional[Dict]:
        return self.data_points.get(identifier)

    def block_reads(self):
        self._allow_reads.set()

    def unblock_reads(self):
        self._allow_reads.clear()

    def get_value(self, identifier: str, disable_cache: bool = False, state_id: Optional[str] = None) -> DataPointValue:
        """
        Gets the current value of a data point by combining all involved provider values.
        :param identifier: The data point identifier
        :param disable_cache: Whether to request cache prevention for all providers
        :param state_id: An optional state id (if supported by provider) to request a potentially older or pinned
                         value.
        :return:
        """
        self._allow_reads.wait()
        dp = self.data_points[identifier]
        source_values = self._get_source_values(dp, disable_cache, state_id)
        if len(source_values) == 0 and "coupling" not in dp:
            if "value" in dp:
                return dp["value"]
            self.logger.warning(f"Data point {identifier=} has no source values")
            return None
        source_values["DP_ID"] = identifier
        if "coupling" in dp:
            return self.s_parser.parse(dp["coupling"], source_values)
        return source_values.get("X1")

    def set_value(self, identifier: str, value: DataPointValue) -> bool:
        dp = self.data_points[identifier]
        dp["value"] = value
        source_values = self._get_source_values(dp)
        source_values["V"] = value
        source_values["DP_ID"] = identifier
        targets = dp["providers"]["targets"] if "targets" in dp["providers"] else []
        success = True
        for index, target in enumerate(targets):
            snippet = target["coupling"] if "coupling" in target else None
            target_value = self.s_parser.parse(snippet, source_values)
            provider = self.get_provider(target["provider_type"])
            success &= provider.set_value(identifier, index, target_value)
        return success

    def find_datapoint_by_provider(self, provider_type: str, provider_data: dict) -> List[str]:
        points = []
        for dp in self.data_points.values():
            identifier = dp["identifier"]
            targets = dp["providers"].get("targets", [])
            sources = dp["providers"].get("sources", [])
            providers = sources + targets
            for p in providers:
                if p["provider_type"] == provider_type:
                    p_data = p["provider_data"]
                    relevant = True
                    for k, v in provider_data.items():
                        if k not in p_data:
                            relevant = False
                            break
                        if p_data[k] != v:
                            relevant = False
                            break
                    if relevant:
                        points.append(identifier)
                        continue
        return points

    def find_datapoint_by_cot(self, cot: COT):
        points = []
        for dp in self.data_points.values():
            if dp["protocol"] != "60870-5-104":
                continue
            p_data = dp["protocol_data"]
            if p_data["cot"] == cot:
                points.append(dp)
        return points

    def start(self):
        self._wait_for_providers()

    def stop(self):
        for _, provider in self.providers.items():
            provider.stop()

    @property
    def next_callback_id(self):
        self._on_change_callback_id += 1
        return self._on_change_callback_id

    def add_on_change_callback(self, callback: Callable[[str, DataPointValue, Optional[str]], None], ids: Optional[Set[str]]) -> int:
        """
        Adds a callback for data point changes.
        The callback is called for each change of a data point that it matches.
        The callback takes the data point identifier (str) and the new value (DataPointValue).

        :param callback: The Callback Callable
        :param ids: An optional set of data point IDs that the callback should be limited to.
        :return: The ID of the callback. Can be used to remove the callback.
        """
        meta = {
            "identifiers": ids,
            "callback": callback
        }
        callback_id = self.next_callback_id
        for provider_type, provider in self.providers.items():
            provider.add_filter_ids(ids)
        self._on_change_callbacks[callback_id] = meta
        return callback_id

    def _on_change(self, identifier: str, value: DataPointValue, state_id: Optional[str] = None,
                   context: Optional[str] = None):
        if context is not None and context != "DP_ID":
            return
        for callback_meta in self._on_change_callbacks.values():
            id_filter = callback_meta.get("identifiers")
            callback = callback_meta.get("callback")
            if id_filter is None or identifier in id_filter:
                callback(identifier, value, state_id)

    def _wait_for_providers(self):
        provider: DataPointProvider
        for provider_type, provider in self.providers.items():
            self.logger.info(f"Initializing Provider of type {provider_type}")
            provider.start()
            provider.add_on_change(self._on_change)

    def _get_source_values(self, dp: dict, disable_cache: bool = False,
                           state_id: Optional[str] = None) -> Dict[str, DataPointValue]:

        sources = dp["providers"]["sources"] if "sources" in dp["providers"] else []
        values: Dict[str, DataPointValue] = {}
        for i, source in enumerate(sources):
            provider = self.get_provider(source["provider_type"])
            val = provider.get_value(dp["identifier"], i, disable_cache=disable_cache, state_id=state_id)
            if "transform" in source:
                val = self.s_parser.parse(source["transform"], {"X": val, "X1": val})
            values[f"X{i+1}"] = val
        return values

    def provider_search(self, provider_type: Optional[str], provider_data: dict) -> Dict[str, Dict]:
        """
        Search for provider-specific configurations
        :param provider_type: The provider type to search for (or None to match any type)
        :param provider_data: The (partial) configuration (provider_data) to filter
        :return: A dict of providers matching the request, keys are data point identifiers
        """
        providers = {}
        for _, point in self.data_points.items():
            if "providers" not in point:
                continue
            for k in point["providers"].keys():
                identifier = point["identifier"]
                for provider in point["providers"][k]:
                    if provider_type is None or provider["provider_type"] == provider_type:
                        matches = True
                        data = provider["provider_data"]
                        for key, value in provider_data.items():
                            if key not in data:
                                matches = False
                                break
                            if data[key] != value:
                                matches = False
                                break
                        if matches:
                            providers.setdefault(identifier, []).append(provider)
        return providers

    def get_provider(self, provider_type: str) -> DataPointProvider:
        if provider_type in self.providers:
            return self.providers[provider_type]
        raise KeyError(f"Unknown provider of type {provider_type}")

    def _init_providers(self):
        for _, point in self.data_points.items():
            if "providers" not in point:
                continue

            for k in point["providers"].keys():
                for provider in point["providers"][k]:
                    t = provider["provider_type"]
                    if t not in self.providers:
                        self.providers[t] = self._init_provider(t)

    def _init_provider(self, provider_type: str) -> DataPointProvider:
        # Dynamically try to load the provider
        provider_type = provider_type.lower()
        provider_name = "".join([s.capitalize() for s in provider_type.split("_")])
        try:
            module = f"wattson.datapoints.providers.{provider_type}_provider"
            cls = f"{provider_name}Provider"
            m = importlib.import_module(module)
            provider_cls: Type[DataPointProvider] = getattr(m, cls)
            config = self.provider_conf.get(provider_type, {})
            config["logger"] = self.logger
            return provider_cls(provider_configuration=config, points=self.data_points, manager=self)
        except Exception:
            raise ValueError(f"Cannot instantiate provider of type {provider_type} as this provider is not installed")

    def _sanitize_datapoints(self):
        for k, dp in self.data_points.items():
            if "providers" not in dp:
                dp["providers"] = {}
