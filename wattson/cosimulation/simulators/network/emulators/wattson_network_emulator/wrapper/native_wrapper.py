import typing

from wattson.cosimulation.simulators.network.components.wattson_network_host import WattsonNetworkHost
from wattson.cosimulation.simulators.network.components.wattson_network_node import WattsonNetworkNode
from wattson.cosimulation.simulators.network.emulators.wattson_network_emulator.wrapper.node_wrapper import NodeWrapper
from wattson.networking.namespaces.namespace import Namespace


class NativeWrapper(NodeWrapper):
    @property
    def node(self) -> WattsonNetworkNode:
        return typing.cast(WattsonNetworkNode, self.entity)

    def get_namespace(self) -> Namespace:
        if self._namespace is None:
            namespace_name = f"w_{self.entity.entity_id}"
            self._namespace = Namespace(name=namespace_name)
        return self._namespace

    def create(self):
        namespace = self.get_namespace()
        if namespace.exists():
            self.logger.error("Namespace already exists")
            return False
        namespace.create()
        if isinstance(self.node, WattsonNetworkHost):
            self.node.loopback_up()

    def clean(self):
        namespace = self.get_namespace()
        if not namespace.exists():
            self.logger.warning("Namespace not found")
            return False
        self.entity.stop()
        namespace.clean()
