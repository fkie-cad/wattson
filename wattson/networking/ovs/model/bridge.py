from typing import List, TYPE_CHECKING, Optional

from natsort import natsorted
from ovs.db.idl import Row

from wattson.networking.ovs.model.data_row import DataRow
from wattson.networking.ovs.model.mac_table_entry import MacTableEntry
from wattson.networking.ovs.model.port import Port

if TYPE_CHECKING:
    from wattson.networking.ovs.ovs_manager import OvsManager


class Bridge(DataRow):
    def __init__(self, idl_row: Row, ovs_manager: 'OvsManager') -> None:
        super().__init__(idl_row, ovs_manager)

        self._ports = []
        for port_row in self._idl_row.ports:
            port = Port(port_row, ovs_manager)
            port.bridge = self
            self._ports.append(port)
        self._ports = natsorted(self._ports, key=lambda x: x.name)
        self._port_by_id = {}
        for port in self._ports:
            for port_id in port.get_port_ids():
                self._port_by_id[port_id] = port

    def get_bridge(self) -> Optional['Bridge']:
        return self

    def get_ports(self) -> List[Port]:
        return self._ports

    @property
    def name(self):
        return self._idl_row.name

    def to_dict(self) -> dict:
        d = super().to_dict()
        d["ports"] = [p.to_dict() for p in self.get_ports()]
        return d

    def get_mac_table(self) -> List[MacTableEntry]:
        entries = []
        result = self.ovs_manager.appctl("fdb/show", self.name)
        if isinstance(result, list):
            for entry in result:
                port: Optional[Port] = self._port_by_id.get(entry["port"])
                if port is not None:
                    port_name = port.name
                else:
                    port_name = ""

                mac_entry = MacTableEntry(age=entry["age"], mac_address=entry["mac"], port=entry["port"], port_name=port_name, vlan=entry["vlan"])
                entries.append(mac_entry)
        return entries
