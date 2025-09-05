import typing

from wattson.cosimulation.simulators.network.components.wattson_network_link import WattsonNetworkLink
from wattson.cosimulation.simulators.network.emulators.wattson_network_emulator.wrapper.entity_wrapper import EntityWrapper
from wattson.cosimulation.simulators.network.emulators.wattson_network_emulator.wrapper.interface_wrapper import InterfaceWrapper
from wattson.networking.namespaces.namespace import Namespace

if typing.TYPE_CHECKING:
    from wattson.cosimulation.simulators.network.emulators.wattson_network_emulator import WattsonNetworkEmulator


class LinkWrapper(EntityWrapper):
    def __init__(self, entity: WattsonNetworkLink, emulator: 'WattsonNetworkEmulator', enable_link_properties: bool = True):
        super().__init__(entity, emulator)
        self._enable_link_properties = enable_link_properties

    def get_namespace(self) -> Namespace:
        raise RuntimeError("Links do not have a dedicated namespace")

    @property
    def link(self) -> WattsonNetworkLink:
        return typing.cast(WattsonNetworkLink, self.entity)

    def create(self):
        link = self.link
        interface_wrapper_a: InterfaceWrapper = typing.cast(InterfaceWrapper, self.emulator.get_wrapper(link.interface_a))
        interface_wrapper_b: InterfaceWrapper = typing.cast(InterfaceWrapper, self.emulator.get_wrapper(link.interface_b))
        # Check if interface already exist
        # TODO: Handle this case for virtual interfaces -> clean them first!
        if interface_wrapper_a.exists():
            self.logger.error(f"Interface {interface_wrapper_a.interface.interface_name} already exists")
            return False
        if interface_wrapper_b.exists():
            self.logger.error(f"Interface {interface_wrapper_b.interface.interface_name} already exists")
            return False
        # Create veth pair
        main_namespace = self.emulator.get_main_namespace()
        code0, lines = main_namespace.exec(["ip", "link", "add", interface_wrapper_a.interface.interface_name,
                                           "type", "veth", "peer", "name", interface_wrapper_b.interface.interface_name])
        if not code0:
            self.logger.error(f"Cannot create link {interface_wrapper_a.interface.interface_name} <-> {interface_wrapper_b.interface.interface_name}")
            self.logger.debug("\n".join(lines))
            return False
        # Move interfaces to namespaces and configure them
        wait_timeout = 2
        if interface_wrapper_a.push_to_namespace():
            interface_wrapper_a.configure(wait_timeout=wait_timeout)
        if interface_wrapper_b.push_to_namespace():
            interface_wrapper_b.configure(wait_timeout=wait_timeout)
        self.link.cached_is_up = True
        return True

    def clean(self):
        link = self.link
        interface_wrapper_a: InterfaceWrapper = typing.cast(InterfaceWrapper, self.emulator.get_wrapper(link.interface_a))
        interface_wrapper_b: InterfaceWrapper = typing.cast(InterfaceWrapper, self.emulator.get_wrapper(link.interface_b))
        interface_wrapper_a.clean()
        interface_wrapper_b.clean()

    def apply_link_properties(self) -> bool:
        if not self._enable_link_properties:
            return True
        link = self.link
        interface_wrapper_a: InterfaceWrapper = typing.cast(InterfaceWrapper, self.emulator.get_wrapper(link.interface_a))
        interface_wrapper_b: InterfaceWrapper = typing.cast(InterfaceWrapper, self.emulator.get_wrapper(link.interface_b))
        success = interface_wrapper_a.apply_tc_properties(link.link_model)
        success &= interface_wrapper_b.apply_tc_properties(link.link_model)
        return success
