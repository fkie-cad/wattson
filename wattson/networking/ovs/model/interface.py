from typing import List, Optional, TYPE_CHECKING

from ovs.db.idl import Row

from wattson.networking.ovs.model.data_row import DataRow

if TYPE_CHECKING:
    from wattson.networking.ovs.model.bridge import Bridge
    from wattson.networking.ovs.ovs_manager import OvsManager


class Interface(DataRow):
    def __init__(self, idl_row: Row, ovs_manager: 'OvsManager'):
        super().__init__(idl_row, ovs_manager)
        self.port = None
        self.update_status("unknown")

    @property
    def name(self):
        return self._idl_row.name

    @property
    def type(self):
        return self._idl_row.type

    def get_port_ids(self) -> List[int]:
        return self._idl_row.ofport

    def update_status(self, status: str):
        old_state = self._status.get("system_link_state")
        self._status["system_link_state"] = status.lower()
        if status.lower() != old_state:
            self._ovs_manager.notify_change(self, {"system_link_state": self._status.get("system_link_state")})

    def get_bridge(self) -> Optional['Bridge']:
        if self.port is not None:
            return self.port.get_bridge()
        return None
