import typing

from powerowl.performance.timing import Timing

from wattson.cosimulation.simulators.network.components.wattson_network_host import WattsonNetworkHost
from wattson.cosimulation.simulators.network.components.wattson_network_node import WattsonNetworkNode
from wattson.cosimulation.simulators.network.emulators.wattson_network_emulator.wrapper.node_wrapper import NodeWrapper
from wattson.networking.namespaces.namespace import Namespace


class NativeWrapper(NodeWrapper):
    @property
    def node(self) -> WattsonNetworkNode:
        return typing.cast(WattsonNetworkNode, self.entity)

    def get_namespace(self) -> Namespace:
        if self.node.is_outside_namespace():
            return self.emulator.get_main_namespace()
        if self._virtual_machine_namespace is None:
            namespace_name = f"w_{self.entity.entity_id}"
            self._namespace = Namespace(name=namespace_name)
        return self._namespace

    def create(self):
        namespace = self.get_namespace()
        if namespace.exists():
            if self.node.is_outside_namespace():
                self.logger.info("Namespace already exists")
                return True
            self.logger.error("Namespace already exists")
            return False
        if not namespace.create():
            return False
        if isinstance(self.node, WattsonNetworkHost):
            self.node.loopback_up()
        return True

    def clean(self):
        namespace = self.get_namespace()
        if not namespace.exists():
            self.logger.warning("Namespace not found")
            return False

        with Timing("Entity stop").as_sum_timing():
            self.entity.stop()
        if not self.node.is_outside_namespace():
            with Timing("Namespace clean").as_sum_timing():
                namespace.clean()
