from typing import List, Optional, TYPE_CHECKING

from ovs.db.idl import Row

from wattson.networking.ovs.model.data_row import DataRow
from wattson.networking.ovs.model.interface import Interface

if TYPE_CHECKING:
    from wattson.networking.ovs.model.bridge import Bridge
    from wattson.networking.ovs.ovs_manager import OvsManager


class Port(DataRow):
    def __init__(self, idl_row: Row, ovs_manager: 'OvsManager'):
        super().__init__(idl_row, ovs_manager)
        self.bridge = None

        interfaces = []
        for interface_row in self._idl_row.interfaces:
            interface = Interface(interface_row, ovs_manager)
            interface.port = self
            interfaces.append(interface)
        self._interfaces = interfaces
        self._port_ids = set()
        for interface in self.get_interfaces():
            self._port_ids.update(interface.get_port_ids())

    def get_interfaces(self) -> List[Interface]:
        return self._interfaces

    def get_port_ids(self) -> list:
        return list(self._port_ids)

    @property
    def name(self):
        return self._idl_row.name

    def to_dict(self) -> dict:
        d = super().to_dict()
        d["interfaces"] = [interface.to_dict() for interface in self.get_interfaces()]
        return d

    def get_bridge(self) -> Optional['Bridge']:
        return self.bridge
