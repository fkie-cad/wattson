import abc
from typing import Optional, TYPE_CHECKING

from ovs.db.idl import Row

if TYPE_CHECKING:
    from wattson.networking.ovs.model.bridge import Bridge
    from wattson.networking.ovs.ovs_manager import OvsManager


class DataRow(abc.ABC):
    table_name: str

    def __init__(self, idl_row: Row, ovs_manager: 'OvsManager'):
        self._idl_row = idl_row
        self._ovs_manager = ovs_manager
        self._status = {}

    @property
    def name(self) -> str:
        return "DataRow"

    def get_uuid(self) -> str:
        return self._idl_row.uuid

    def columns(self) -> list:
        return self._idl_row._table.columns

    def to_dict(self):
        row_dict = {k: self._idl_row.__getattr__(k) for k in self.columns()}
        row_dict.update(self._status)
        return row_dict

    def get_bridge(self) -> Optional['Bridge']:
        return None

    def parse_update(self, update: Row):
        changes = {}
        old_values = {}
        new_values = {}
        for c in update._table.columns:
            if hasattr(update, c):
                if hasattr(self._idl_row, c):
                    new_value = getattr(self._idl_row, c)
                else:
                    new_value = None

                old_values[c] = getattr(update, c)
                new_values[c] = new_value

                changes[c] = {
                    "old_value": getattr(update, c),
                    "new_value": new_value
                }
        self.on_change(changes, old_values, new_values)

    def on_change(self, changes: dict, old_values: dict, new_values: dict):
        self._ovs_manager.notify_change(self, new_values)
