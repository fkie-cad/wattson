import copy
import json
import traceback
import warnings
from typing import List, Any, TYPE_CHECKING, Optional

from wattson.cosimulation.exceptions import ExpansionException
from wattson.services.configuration.configuration_store import ConfigurationStore
from wattson.services.configuration.service_configuration import ServiceConfiguration
from wattson.services.service_priority import ServicePriority

if TYPE_CHECKING:
    from wattson.cosimulation.simulators.network.components.wattson_network_node import WattsonNetworkNode


class ConfigurationExpander:
    """
    Expands the configuration of a WattsonService by replacing expansion placeholders with
    their current or predefined values.
    """
    def __init__(self, configuration_store: ConfigurationStore):
        self.configuration_store = configuration_store

    def expand_node_configuration(self, node: 'WattsonNetworkNode',
                                  service_configuration: ServiceConfiguration) -> ServiceConfiguration:
        """
        Expands the given ServiceConfiguration to a fresh ServiceConfiguration where all expansion handles
        are replaced by their actual value with context information provided by the ExpansionStore.
        If an undefined expansion handle is found, an ExpansionException is raised.

        :param node: The node to expand this configuration for
        :param service_configuration: The ServiceConfiguration to expand
        :return: A fresh ServiceConfiguration with all expansion handles replaced
        """
        try:
            expanded_configuration = ServiceConfiguration()
            for key, value in service_configuration.items():
                try:
                    expanded_configuration[key] = copy.deepcopy(value)
                except Exception as e:
                    print(f"{node.node_id}: {key}")
                    print(f"{type(value)}")
                    print(f"{repr(value)}")
                    for i in value:
                        print(type(i))
                        print(repr(i))
                    raise e
            self._replace_short_notations(expanded_configuration)
            self._expand_configuration(node, expanded_configuration)
            return expanded_configuration
        except ExpansionException as e:
            # warnings.warn(json.dumps(service_configuration, indent=4))
            raise e

    def _replace_short_notations(self, expanded_configuration: ServiceConfiguration):
        short_notation_priority = sorted(list(self.configuration_store.short_notations.keys()),
                                         key=lambda e: len(e),
                                         reverse=True)
        for key, value in expanded_configuration.items():
            expanded_configuration[key] = self._recursively_replace_short_notations(
                short_notation_priority, value
            )

    def _recursively_replace_short_notations(self, short_notation_priority: list, configuration):
        if isinstance(configuration, dict):
            for key, value in configuration.items():
                configuration[key] = self._recursively_replace_short_notations(short_notation_priority, value)
            return configuration
        if isinstance(configuration, list):
            resolved_list = []
            for value in configuration:
                resolved_list.append(self._recursively_replace_short_notations(short_notation_priority, value))
            return resolved_list
        if isinstance(configuration, str):
            return self._resolve_short_notation(configuration=configuration,
                                                short_notation_priority=short_notation_priority)
        return configuration

    def _resolve_short_notation(self, configuration: str, short_notation_priority: list) -> str:
        # Full coverage / stand alone
        if configuration in self.configuration_store.short_notations:
            return self.configuration_store.short_notations[configuration]
        # Search for partial occurrence
        for short_notation in short_notation_priority:
            if short_notation in configuration:
                return configuration.replace(
                    short_notation,
                    self.configuration_store.short_notations[short_notation]
                )
        return configuration

    def _expand_configuration(self, node: 'WattsonNetworkNode', expanded_configuration: ServiceConfiguration):
        self._recursively_expand_configuration(node, expanded_configuration)

    def _recursively_expand_configuration(self, node: 'WattsonNetworkNode', configuration, path: Optional[List] = None):
        if path is None:
            path = []
        try:
            if isinstance(configuration, dict):
                for key, value in configuration.items():
                    configuration[key] = self._recursively_expand_configuration(node, value, path + [key])
                return configuration
            if isinstance(configuration, list):
                for i, value in enumerate(configuration):
                    configuration[i] = self._recursively_expand_configuration(node, value, path + [i])
                return configuration
            if isinstance(configuration, (float, int, bool)):
                return configuration
            if isinstance(configuration, ServicePriority):
                return configuration.get_global(node=node)
            if isinstance(configuration, str):
                if configuration.startswith("!"):
                    parts = configuration.split(".")
                    return self._resolve_expansion_parts(node, parts)
                return configuration
        except Exception as e:
            warnings.warn(f"Failed to handle expansion: {path=} ({type(configuration)})")
            traceback.print_exception(e)
            raise e

        # Default - should not happen
        warnings.warn(f"Configuration expansion handled by default case, which should not happen. {path=} // {type(configuration)} // {repr(configuration)}")
        return configuration

    def _resolve_expansion_parts(self, node: 'WattsonNetworkNode', parts: List[str]) -> Any:
        first_part = parts.pop(0)
        resolved_first_part = self._resolve_single_expansion(node, first_part)

        if len(parts) > 0:
            postfix = self._resolve_expansion_parts(node, parts)
            if isinstance(resolved_first_part, dict):
                if postfix not in resolved_first_part:
                    raise ExpansionException(f"Expansion {resolved_first_part} has no key {postfix} ({parts})")
                return resolved_first_part[postfix]
            if isinstance(resolved_first_part, list):
                try:
                    index = int(postfix)
                except ValueError:
                    raise ExpansionException(
                        f"Expanded list index is no valid integer: {postfix} ({parts})"
                    )
                if 0 <= index < len(resolved_first_part):
                    return resolved_first_part[index]
                raise ExpansionException(
                    f"Cannot access index {postfix} ({parts} of list of length {len(resolved_first_part)})"
                )
            raise ExpansionException(f"Cannot combine prefix {resolved_first_part} with {postfix} ({parts})")
        else:
            return resolved_first_part

    def _resolve_single_expansion(self, node: 'WattsonNetworkNode', expansion_string: str) -> Any:
        if not expansion_string.startswith("!"):
            return expansion_string
        if expansion_string in self.configuration_store:
            replacement = self.configuration_store[expansion_string]
            if callable(replacement):
                # Resolve using callback
                return replacement(node, self.configuration_store)
            # Direct resolving
            return replacement
        # Syntax matches an expansion, but no fitting expansion exists. Might be valid, but we issue a warning.
        warnings.warn(f"Potential expansion string without defined expansion: {expansion_string}")
        return expansion_string
