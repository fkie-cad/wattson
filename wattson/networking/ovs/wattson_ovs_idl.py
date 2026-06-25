from typing import Optional, Callable

import ovs.db.idl


class WattsonOvsIdl(ovs.db.idl.Idl):
    def __init__(self, remote, schema_helper, probe_interval=None, leader_only=True, on_notify: Optional[Callable] = None):
        super().__init__(remote, schema_helper, probe_interval, leader_only)
        self._on_notify = on_notify

    def notify(self, event, row, updates=None):
        if callable(self._on_notify):
            self._on_notify(event, row, updates)
        super().notify(event, row, updates)
