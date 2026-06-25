import json
import logging
import shlex
import subprocess
import threading
import time
from typing import Optional, Any, List, Callable

import errno
import ovs.poller
from natsort import natsorted
from ovs.db.idl import Idl, SchemaHelper, Row
from ovs.jsonrpc import Connection as RpcConnection
from ovs.stream import Stream
from ovs.jsonrpc import Message as RpcMessage
from pyroute2 import IPRoute

from wattson.networking.ovs.model.data_row import DataRow
from wattson.networking.ovs.wattson_ovs_idl import WattsonOvsIdl
from wattson.networking.ovs.model.bridge import Bridge
from wattson.networking.ovs.model.interface import Interface
from wattson.util import get_logger


class OvsManager(threading.Thread):
    @staticmethod
    def expose_remote(remote: str):
        """
        Exposes the (locally available) OVS database via unix sockets or TCP.
        Args:
            remote: The path/socket to expose OVS to. E.g., tcp:127.0.0.1:6640 or unix:path/to/db.sock
        """
        cmd = [
            "ovs-vsctl",
            "set-manager",
            shlex.quote(remote),
        ]

    @staticmethod
    def clear_exposed_remotes():
        cmd = [
            "ovs-vsctl",
        ]

    def __init__(self, remote: str, switches: Optional[list] = None, logger: Optional[logging.Logger] = None):
        super().__init__()
        self._remote = remote
        self._switches = switches
        self._logger = logger or get_logger("OvsManager")
        self._logger.setLevel(logging.DEBUG)

        self._stream: Optional[Stream] = None
        self._rpc: Optional[RpcConnection] = None
        self._idl: Optional[WattsonOvsIdl] = None
        self._stop_requested: threading.Event = threading.Event()
        self._ready_event: threading.Event = threading.Event()
        self._init_event: threading.Event = threading.Event()

        self._bridges = []
        self._interface_map = {}
        self._data_row_map = {}

        self._poll_monitor_thread: Optional[threading.Thread] = None
        self._event_monitor_thread: Optional[threading.Thread] = None
        self._monitor_enabled = threading.Event()
        self._monitor_interval = 2

        self._on_change_listeners: List[Callable[[DataRow, dict], None]] = []

    def add_on_change_listener(self, callback: Callable[[DataRow, dict], None]):
        self._on_change_listeners.append(callback)

    def start(self):
        super().start()
        self.start_monitor()
        self._stop_requested.clear()

    def stop(self):
        self._stop_requested.set()
        if self._rpc is not None:
            self._rpc.close()
        self._stream = None
        self._rpc = None

    def run(self):
        self._logger.debug(f"Starting OvsManager against {self._remote}")
        self._ready_event.clear()
        error, stream = Stream.open_block(Stream.open(self._remote))
        if error:
            error_name = errno.errorcode[error]
            self._logger.error(f"Error {error}: {error_name}")
            return
        self._stream = stream
        self._rpc = RpcConnection(self._stream)
        schema = self.get_schema()
        schema_helper = SchemaHelper(schema_json=schema)
        schema_helper.register_all()
        self._logger.debug("Initializing IDL")
        self._idl = WattsonOvsIdl(self._remote, schema_helper=schema_helper, on_notify=self._on_ovs_event)
        # Poll Database
        while not self._idl.has_ever_connected() and not self._stop_requested.is_set():
            poller = ovs.poller.Poller()
            self._idl.wait(poller)
            poller.block()
            self._idl.run()
        self._logger.debug("OvsManager ready")
        self._ready_event.set()

        self.get_bridges()
        self._init_event.set()

        self._stop_requested.wait()
        self._ready_event.clear()
        self._init_event.clear()
        # Shutdown
        self._logger.debug(f"Stopping OvsManager")
        if self._rpc is not None:
            self._rpc.close()
        if self._idl is not None:
            self._idl.close()
        self._rpc = None
        self._idl = None
        self._stream = None

    def matches_bridge(self, bridge: str) -> bool:
        if self._switches is None:
            return True
        return bridge in self._switches

    def is_ready(self) -> bool:
        return self._ready_event.is_set()

    def ensure_ready(self):
        if not self.is_ready():
            raise RuntimeError("OvsManager is not ready")

    def ensure_rpc(self):
        if self._rpc is None:
            raise RuntimeError("OvsManager RPC is not ready")

    def wait_ready(self):
        self._ready_event.wait()

    def rpc_request(self, method: str, params: Any):
        self.ensure_rpc()
        request = RpcMessage.create_request(method=method, params=params)
        error, reply = self._rpc.transact_block(request)
        if error:
            self._logger.error(error)
            return None
        return reply.result

    def get_schema(self):
        schema = self.rpc_request("get_schema", ["Open_vSwitch"])
        if schema is None:
            raise ValueError("Could not get schema")
        return schema

    def get_bridges(self) -> List[Bridge]:
        if len(self._bridges) > 0:
            return self._bridges
        self.wait_ready()
        bridges = []

        for bridge in self._idl.tables["Bridge"].rows.values():
            if not self.matches_bridge(bridge.name):
                continue
            bridges.append(Bridge(bridge, self))
        bridges = natsorted(bridges, key=lambda x: x.name)
        self._bridges = bridges
        return bridges

    def get_bridge(self, name: str) -> Bridge:
        bridges = self.get_bridges()
        for bridge in bridges:
            if bridge.name == name:
                return bridge
        raise KeyError(f"Bridge {name} not found")

    def get_tables(self) -> List[str]:
        self.ensure_ready()
        return list(self._idl.tables.keys())

    def get_table_rows(self, name: str) -> dict:
        self.ensure_ready()
        return dict(self._idl.tables[name].rows.items())

    def appctl(self, *args) -> Any:
        cmd = ["ovs-appctl", "-f", "json"] + list(args)
        return self._json_proc(cmd)

    def _json_proc(self, cmd):
        try:
            result = subprocess.check_output(cmd)
            return json.loads(result)
        except Exception as e:
            return None

    def start_monitor(self):
        if self._poll_monitor_thread is None:
            self._poll_monitor_thread = threading.Thread(target=self._poll_monitor)
            self._poll_monitor_thread.daemon = True
            self._poll_monitor_thread.start()
        if self._event_monitor_thread is None:
            self._event_monitor_thread = threading.Thread(target=self._event_monitor)
            self._event_monitor_thread.daemon = True
            self._event_monitor_thread.start()
        self._monitor_enabled.set()

    def disable_monitor(self):
        self._monitor_enabled.clear()

    def get_interface_by_name(self, name: str) -> Optional[Interface]:
        if name not in self._interface_map:
            self._build_interface_map()
        return self._interface_map.get(name)

    def get_data_row_by_uuid(self, uuid: str) -> Optional[DataRow]:
        if uuid not in self._data_row_map:
            self._build_data_row_map()
        if uuid not in self._data_row_map:
            self._data_row_map[uuid] = None
        return self._data_row_map.get(uuid)

    def _build_interface_map(self):
        self._interface_map = {}
        for bridge in self.get_bridges():
            for port in bridge.get_ports():
                for interface in port.get_interfaces():
                    self._interface_map[interface.name] = interface

    def _build_data_row_map(self):
        # self._data_row_map = {}
        for bridge in self.get_bridges():
            self._data_row_map[bridge.get_uuid()] = bridge
            for port in bridge.get_ports():
                self._data_row_map[port.get_uuid()] = port
                for interface in port.get_interfaces():
                    self._data_row_map[interface.get_uuid()] = interface

    def _poll_monitor(self):
        self.wait_ready()
        self._init_event.wait()
        self._init_status()
        while not self._stop_requested.is_set():
            if self._monitor_enabled.wait(timeout=1):
                self._update_status()
            time.sleep(self._monitor_interval)

    def _event_monitor(self):
        with IPRoute() as ipr:
            ipr.bind()
            while not self._stop_requested.is_set():
                for message in ipr.get(terminate=self._stop_requested.is_set):
                    event = message.get("event")

                    if event in ["RTM_NEWNEIGH", "RTM_DELNEIGH"]:
                        pass

                    if event in ["RTM_NEWLINK"]:
                        attrs = dict(message["attrs"])
                        ifname = attrs.get("IFLA_IFNAME")
                        operstate = attrs.get("IFLA_OPERSTATE")

                        if ifname is None:
                            continue

                        interface = self.get_interface_by_name(ifname)
                        if interface is None:
                            continue
                        interface.update_status(operstate)
                        self._logger.debug(f"{interface.name} -> {operstate}")

    def _update_status(self):
        self._idl.run()

    def _init_status(self):
        link_info = self._json_proc(["ip", "-j", "link", "show"])
        if isinstance(link_info, list):
            for info in link_info:
                interface = self.get_interface_by_name(info["ifname"])
                if interface is None:
                    continue
                interface.update_status(info["operstate"])
        self._update_status()

    def _on_ovs_event(self, event, row, updates=None):
        if event == "update":
            uuid = row.uuid
            data_row = self.get_data_row_by_uuid(uuid)
            if data_row is None:
                return
            bridge = data_row.get_bridge()
            if bridge is None:
                return
            if not self.matches_bridge(bridge.name):
                return
            data_row.parse_update(updates)

        else:
            pass
            """
            self._logger.debug(f"OVS event: {event}")
            self._logger.debug(f"  {repr(row)}")
            self._logger.debug(f"  {updates}")
            """

    def notify_change(self, data_row: DataRow, new_values: dict):
        for callback in self._on_change_listeners:
            try:
                callback(data_row, new_values)
            except Exception as e:
                self._logger.error(f"Error in Callback: {e}")
