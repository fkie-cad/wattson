import queue
import threading
from typing import Dict, Optional

from wattson.apps.interface.clients import CombiClient
from wattson.apps.interface.util.messages import ProcessInfoMonitoring, ProcessInfoControl
from wattson.iec104.interface.types import COT
from wattson.services.deployment import PythonDeployment
from wattson.util import get_logger


class CloseSwitches(PythonDeployment):
    def __init__(self, configuration: Dict):
        super().__init__(configuration)
        self._mtu_ip = self.config.get("mtu_ip", "127.0.0.1")
        self._data_points = self.config.get("data_points", [])
        self._set_points_map = {}
        self._static_map: Optional[Dict] = self.config.get("static_map")
        self._reaction_delay = self.config.get("reaction_delay", 0)
        self.logger = get_logger("CloseSwitches", "CloseSwitches")
        self._terminate_requested = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self):
        self.logger.info("Starting Close Switch Logic")
        self._terminate_requested.clear()
        self._thread = threading.Thread(target=self._close_switches)
        self._thread.start()
        self._thread.join()

    def _close_switches(self):
        if self._static_map is not None:
            self._set_points_map = self._static_map
        else:
            self._build_set_point_map()

        set_point_map = self._set_points_map
        self.logger.info(f"Observing {len(set_point_map)} switch(es)")
        send_queue = queue.Queue()

        def handle_auto_switch_close(update: ProcessInfoMonitoring, ref_arg=False):
            coa = update.coa
            for ioa, value in update.val_map.items():
                if f"{coa}.{ioa}" in set_point_map:
                    self.logger.info(f"Got {(coa, ioa)} = {value=}")
                    set_ioa = set_point_map[f"{coa}.{ioa}"]
                    if value is False:
                        # Close the switch again
                        _cmd = ProcessInfoControl(
                            coa=coa,
                            type_ID=45,
                            cot=COT.ACTIVATION,
                            val_map={set_ioa: True}
                        )
                        send_queue.put(_cmd)

        client = CombiClient(
            node_id="close_switches",
            mtu_ip=self._mtu_ip,
            on_dp_update=handle_auto_switch_close
        )
        client.start()
        while not self._terminate_requested.is_set():
            try:
                cmd = send_queue.get(block=True, timeout=0.5)
                if self._reaction_delay > 0:
                    if self._terminate_requested.wait(self._reaction_delay):
                        break
                self.logger.info(f"Setting {cmd.coa}.{list(cmd.val_map.keys())[0]} = True")
                client.send_cmd(msg=cmd)
            except queue.Empty:
                pass
        client.stop()

    def stop(self):
        self.logger.info("Stopping Close Switches Logic...")
        super().stop()
        self._terminate_requested.set()
        if self._thread is not None:
            self._thread.join()
        self.logger.info("Stopped Close Switches Logic...")

    def _build_set_point_map(self):
        switch_data_points = dict()

        def extract_switch_dict(_data_point) -> Optional[dict]:
            monitoring = None
            control = None
            switch = None
            for p_type in ["sources", "targets"]:
                if p_type in _data_point.get("providers", {}):
                    for provider in _data_point["providers"][p_type]:
                        if provider["provider_type"] != "POWER_GRID":
                            continue
                        data = provider["provider_data"]
                        element = str(data["grid_element"])
                        if data.get("attribute") not in ["closed", "is_closed"]:
                            continue
                        if element.startswith("switch."):
                            switch = element
                            _info = {"coa": _data_point["protocol_data"].get("coa"), "ioa": _data_point["protocol_data"].get("ioa")}
                            if p_type == "sources":
                                monitoring = _info
                            elif p_type == "targets":
                                control = _info
            if switch is not None:
                d = {"switch": switch}
                if monitoring is not None:
                    d["monitoring"] = monitoring
                if control is not None:
                    d["control"] = control
                return d
            return None

        for data_point in self._data_points:
            switch_data = extract_switch_dict(data_point)
            if switch_data is None:
                continue
            if switch_data["switch"] in switch_data_points:
                switch_data_points[switch_data["switch"]].update(switch_data)
            else:
                switch_data_points[switch_data["switch"]] = switch_data

        set_point_map = {}
        for switch_id, info in switch_data_points.items():
            if "monitoring" in info and "control" in info:
                m = info["monitoring"]
                c = info["control"]
                set_point_map[f"{m['coa']}.{m['ioa']}"] = f"{c['ioa']}"
            else:
                self.logger.warning(f"Switch {switch_id} is either not monitorable or controllable")

        self._set_points_map = set_point_map
