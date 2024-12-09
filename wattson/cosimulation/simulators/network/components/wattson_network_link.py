import dataclasses
import time
import typing
from typing import TYPE_CHECKING, Optional, Any, Callable, List, ClassVar

import json

from wattson.cosimulation.simulators.network.components.interface.network_link import NetworkLink
from wattson.cosimulation.simulators.network.components.remote.remote_network_entity_representation import RemoteNetworkEntityRepresentation
from wattson.cosimulation.simulators.network.components.wattson_network_entity import WattsonNetworkEntity
from wattson.cosimulation.simulators.network.components.network_link_model import NetworkLinkModel

if TYPE_CHECKING:
    from wattson.cosimulation.simulators.network.components.wattson_network_interface import WattsonNetworkInterface


@dataclasses.dataclass(kw_only=True)
class WattsonNetworkLink(WattsonNetworkEntity, NetworkLink):
    interface_a: Optional['WattsonNetworkInterface'] = None
    interface_b: Optional['WattsonNetworkInterface'] = None
    link_model: NetworkLinkModel = dataclasses.field(default_factory=lambda: NetworkLinkModel())
    config: dict = dataclasses.field(default_factory=lambda: {})
    link_type: str = "digital"

    class_id: ClassVar[int] = 0

    def __post_init__(self):
        super().__post_init__()
        self.cached_is_up: Optional[bool] = None
        self._on_link_property_change_callbacks: List[Callable[['WattsonNetworkLink', str, Any], None]] = []
        # Apply link model
        bandwidth = self.config.get("bandwidth")
        if bandwidth is not None:
            self.link_model.set_bandwidth_from_string(bandwidth)
        delay = self.config.get("delay")
        if delay is not None:
            self.link_model.set_delay_from_timespan(delay)
        jitter = self.config.get("jitter")
        if jitter is not None:
            self.link_model.set_jitter_from_timespan(jitter)
        bit_error_rate = self.config.get("bit-error-rate")
        if bit_error_rate is not None:
            self.link_model.set_bit_error_rate_from_string(bit_error_rate)
        packet_loss = self.config.get("packet-loss")
        if packet_loss is not None:
            self.link_model.set_packet_loss_from_string(packet_loss)
        self.link_model.set_on_change_callback(self._on_link_property_change)

    def get_prefix(self) -> str:
        return "l"

    def _on_link_property_change(self, link_property: str, value: Any):
        for callback in self._on_link_property_change_callbacks:
            callback(self, link_property, value)

    def add_on_link_property_change_callback(self, callback: Callable[['WattsonNetworkLink', str, Any], None]):
        """
        Sets a callback to be called whenever a link model property is updated.
        @param callback: The callback to call. Takes the link instance, the changed property name and the new value
        @return:
        """
        self._on_link_property_change_callbacks.append(callback)

    def get_link_model(self) -> NetworkLinkModel:
        return self.link_model

    @property
    def entity_id(self) -> str:
        return self.link_id

    @property
    def link_id(self):
        return self.prefix_id(self.id)

    def to_remote_representation(self, force_state_synchronization: bool = True) -> RemoteNetworkEntityRepresentation:
        d = super().to_remote_representation(force_state_synchronization)
        d.update({
            "class": self.__class__.__name__,
            "entity_id": self.entity_id,
            "is_up": self.is_up(force_update=force_state_synchronization),
            "link_model": self.link_model.to_remote_representation(force_state_synchronization),
            "interface_a_id": self.interface_a.entity_id,
            "interface_b_id": self.interface_b.entity_id
        })
        return d

    @staticmethod
    def prefix_id(link_id) -> str:
        if link_id[0] in "0123456789":
            return f"n{link_id}"
        return link_id

    def get_interface_a(self) -> 'WattsonNetworkInterface':
        return self.interface_a

    def get_interface_b(self) -> 'WattsonNetworkInterface':
        return self.interface_b
    
    def get_other_interface(self, interface: 'WattsonNetworkInterface') -> 'WattsonNetworkInterface':
        from wattson.cosimulation.simulators.network.components.wattson_network_interface import WattsonNetworkInterface
        return typing.cast(WattsonNetworkInterface, super().get_other_interface(interface))

    def up(self):
        if not self.is_started:
            self.logger.error("Cannot set link up")
        node_a = self.interface_a.get_node()
        node_a.exec(["ip", "link", "set", self.interface_a.get_system_name(), "up"])
        self.cached_is_up = True
        self.interface_a.up()
        self.interface_b.up()
        self.network_emulator.on_topology_change(self, "link_up")

    def down(self):
        if not self.is_started:
            self.logger.error("Cannot set link down")
        self.cached_is_up = False
        node_a = self.interface_a.get_node()
        node_a.exec(["ip", "link", "set", self.interface_a.get_system_name(), "down"])
        self.interface_a.down()
        self.interface_b.down()
        self.network_emulator.on_topology_change(self, "link_down")

    def is_up(self, force_update: bool = True) -> bool:
        if not force_update and self.cached_is_up is not None:
            return self.cached_is_up
        state = self.get_link_state().get("state", {})
        flags = state.get("flags", [])
        self.cached_is_up = False
        if "UP" in flags and "LOWER_UP" in flags:
            self.cached_is_up = True
        return self.cached_is_up

    def get_link_state(self) -> dict:
        if not self.is_started:
            self.logger.error("Cannot get link state")
            return {"result": "Error"}
        node_a = self.interface_a.get_node()
        code, output = node_a.exec(["ip", "--json", "link", "show", self.interface_a.get_system_name()])
        state = {}
        try:
            state = json.loads("".join(output))[0]
        except Exception as e:
            self.logger.debug(f"{''.join(output)}")
            self.logger.debug(f"{e=}")
        finally:
            return {
                "result": "\n".join(output),
                "state": state
            }
