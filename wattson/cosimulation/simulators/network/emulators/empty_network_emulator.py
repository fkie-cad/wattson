from typing import Optional, Union, Type

from wattson.cosimulation.control.messages.wattson_query import WattsonQuery
from wattson.cosimulation.control.messages.wattson_response import WattsonResponse
from wattson.cosimulation.exceptions import InvalidSimulationControlQueryException
from wattson.cosimulation.simulators.network.components.wattson_network_entity import WattsonNetworkEntity
from wattson.cosimulation.simulators.network.network_emulator import NetworkEmulator
from wattson.networking.namespaces.namespace import Namespace


class EmptyNetworkEmulator(NetworkEmulator):
    def start(self):
        pass

    def stop(self):
        pass

    def cli(self):
        pass

    def deploy_services(self):
        pass

    def get_namespace(self, node: Union[str, WattsonNetworkEntity], raise_exception: bool = True) -> Optional[Namespace]:
        return None

    def handles_simulation_query_type(self, query: Union[WattsonQuery, Type[WattsonQuery]]) -> bool:
        return False

    def handle_simulation_control_query(self, query: WattsonQuery) -> Optional[WattsonResponse]:
        raise InvalidSimulationControlQueryException(
            f"EmptyNetworkEmulator does not handle {query.__class__.__name__}"
        )
