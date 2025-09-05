import threading
from pathlib import Path
from typing import Optional
from c104 import Cot

from wattson.datapoints.interface import DataPointValue
from wattson.hosts.rtu.rtu_logic import RTULogic
from wattson.iec104.interface.server import IECServerInterface
from wattson.iec104.interface.types import COT


class SpontaneousLogic(RTULogic):
    """RTU Logic that issues spontaneous transmissions for changes of spontaneous data points"""
    def __init__(self, rtu: 'RTU', **kwargs):
        super().__init__(rtu, **kwargs)
        self._monitor_interval = kwargs.get("monitor_interval", 10)
        self._terminate = threading.Event()
        self._monitored_dps = {}

    def on_start(self):
        super().on_start()
        monitored_dps = self.rtu.manager.find_datapoint_by_cot(COT.SPONTANEOUS)
        monitored_ids = {dp["identifier"] for dp in monitored_dps}
        self.logger.info(monitored_ids)
        self._monitored_dps = {dp["identifier"]: dp for dp in monitored_dps}
        self.rtu.manager.add_on_change_callback(callback=self._on_change, ids=monitored_ids)

    def on_stop(self):
        super().on_stop()
        self._terminate.set()

    def _on_change(self, identifier: str, _: DataPointValue, state_id: Optional[str]):
        dp = self._monitored_dps.get(identifier)
        if dp is None:
            self.logger.error(f"Cannot find data point {identifier}")
            return
        self._send_spontaneous(dp["protocol_data"]["ioa"], identifier, state_id)

    def join(self, timeout: Optional[float] = None):
        self._terminate.wait(timeout)

    def configure(self):
        super().configure()

    def _send_spontaneous(self, ioa, identifier, state_id: Optional[str]):
        self.logger.info(f"Sending spontaneous: {ioa} // {identifier}")
        server: IECServerInterface = self.rtu.get_104_socket()
        point = server.get_datapoint(ioa)
        self.logger.info(f" Spontaneous point: {type(point)} -- {repr(point.value)}")
        point.value = self.rtu.manager.get_value(
            identifier=identifier,
            disable_cache=True,
            state_id=state_id
        )
        point.transmit(Cot.SPONTANEOUS)
