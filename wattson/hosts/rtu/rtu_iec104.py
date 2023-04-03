import datetime
import logging
from typing import Optional, TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    from wattson.hosts.rtu import RTU
from wattson.iec104.common.datapoint import IEC104Point
from wattson.iec104.common.iec104message import IEC104Message
from wattson.iec104.interface.apdus import APDU, I_FORMAT
from wattson.iec104.interface.types import TypeID, Step
from wattson.iec104.common.config import SERVER_UPDATE_PERIOD_MS


class RtuIec104:
    def __init__(self, rtu: 'RTU', **kwargs):
        self.rtu = rtu
        self.port = kwargs.get("port", 2404)
        self.logger = self.rtu.logger.getChild("IEC104")
        self.periodic_update_ms = kwargs.get("periodic_update_ms", SERVER_UPDATE_PERIOD_MS)
        self.periodic_update_start = kwargs.get("periodic_update_start", 0)
        self.periodic_updates_enable = kwargs.get("periodic_updates_enable", True)
        if not self.periodic_updates_enable:
            self.logger.info("Globally disabling periodic updates")
        self.server = None

    def setup_socket(self):
        def update_datapoint(point: IEC104Point):
            identifier = f"{point.coa}.{point.ioa}"
            try:
                val = self.rtu.manager.get_value(identifier)
                point.value = val
            except Exception as e:
                self.logger.error(f"Error reading {identifier}: {e}")

        def on_unexpected_msg(server, message, cause):
            self.logger.warning(f"Received unexpected, likely bad msg with cause {cause}: {message.type}")
                                #f"{message.cot} {message.ioa} {message.value} {message.quality}")

        def on_clock_synch(new_ts: datetime.datetime):
            self.logger.debug(f"Recvd new ts: {new_ts}")

        def on_receive_apdu(apdu: APDU):
            if isinstance(apdu, I_FORMAT):
                if apdu.type == 100:
                    self.logger.info("Received C_IC_NA_1 (General Interrogation)")

                if apdu.type == 102:    # READ
                    self.rtu.statistics.log(str(self.rtu.coa), event_class="apdu.monitoring.request", value="receive",
                                            data={"type": apdu.type, "cot": apdu.cot}
                                            )
                else:
                    self.rtu.statistics.log(str(self.rtu.coa), event_class="apdu.control.request", value="receive",
                                            data={"type": apdu.type, "cot": apdu.cot}
                                            )

        def on_send_apdu(apdu: APDU):
            if isinstance(apdu, I_FORMAT):
                if apdu.type < 45:    # READ
                    self.rtu.statistics.log(str(self.rtu.coa), event_class="apdu.monitoring.response", value="send",
                                            data={"type": apdu.type, "cot": apdu.cot}
                                            )
                else:
                    self.rtu.statistics.log(str(self.rtu.coa), event_class="apdu.control.response", value="send",
                                            data={"type": apdu.type, "cot": apdu.cot}
                                            )

        def on_connect(server, ip: str):
            self.logger.info(f"Client {ip} connecting")
            return True

        # Does not yet send End-of-Interro
        # https://gitlab.fit.fraunhofer.de/de.tools/104-connector-python/-/blob/master/src/Server.cpp#L693
        self.logger.info(f"Adding Server Socket: {self.rtu.ip}:{self.port}")
        self.server = self.rtu.iec_server_class(
            self.rtu,
            self.rtu.ip,
            port=self.port,
            datapoints=self._get_data_points(),
            pre_init_datapoints=True,
            on_before_read=update_datapoint,
            log_raw=self.rtu.logger.level == logging.DEBUG,
            on_before_auto_transmit=update_datapoint,
            on_setpoint_command=self.set_datapoint,
            on_step_command=self.set_datapoint,
            on_unexpected_msg=on_unexpected_msg,
            on_receive_apdu=on_receive_apdu,
            on_send_apdu=on_send_apdu,
            on_clock_synch=on_clock_synch,
            on_connect=on_connect,
            periodic_update_ms=self.periodic_update_ms,
            periodic_update_start=self.periodic_update_start,
            periodic_updates_enable=self.periodic_updates_enable
        )

    def start(self):
        self.server.start()

    def stop(self):
        self.server.stop()

    def _get_data_points(self):
        data_points = []
        for identifier, dp in self.rtu.data_point_dict.items():
            if dp["protocol"] == "60870-5-104":
                data_points.append(dp)
        return data_points

    def set_datapoint(self, point: IEC104Point,
                      prev: Optional[IEC104Point] = None,
                      message: IEC104Message = None):
        self.logger.info(f"Datapoint Update (Set) for ioa {point.ioa} with value {point.value}")
        identifier = f"{point.coa}.{point.ioa}"

        try:
            # split due to not require c104 import
            # TODO: Handle Quality
            sufficient_quality = True
            if sufficient_quality:
                # noinspection DuplicatedCode
                if point.type not in (TypeID.C_RC_NA_1, TypeID.C_RC_TA_1):
                    res = self.rtu.manager.set_value(identifier, point.value)
                else:
                    value = self.rtu.manager.get_value(identifier)
                    if value is None or np.isnan(value):
                        value = 0
                    if point.value == Step.LOWER:
                        value -= 1
                    elif point.value == Step.HIGHER:
                        value += 1
                    else:
                        self.logger.warning(f"Received bad IO in step command with point {point}")
                        return False
                    self.logger.info(f"Step Command with value {point.value} results in value {value}")
                    res = self.rtu.manager.set_value(identifier, value)
            else:
                self.logger.info(f"Bad quality of point: {point.quality}, of msg: {message.quality}"
                                 f" prev: {prev}")
                self.logger.warning("Until Quality bug is fixed, still write IO")
                # return self._handle_bad_quality_set_dp(point, message)
                return False

            return res
        except Exception as e:
            self.logger.error(f"Failed to update dp {identifier}: {e}")
            raise e
