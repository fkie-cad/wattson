import datetime
import logging
import threading
import time
from typing import TYPE_CHECKING

import c104

from wattson.util import log_contexts

from wattson.iec104.common.config import *
from wattson.iec104.interface.types import COT
from wattson.iec104.interface.server import IECServerInterface
from wattson.iec104.implementations.c104 import C104Point, build_apdu_from_c104_bytes

if TYPE_CHECKING:
    from wattson.hosts.rtu.rtu import RTU


class IEC104Server(IECServerInterface):
    def __init__(self, rtu: 'RTU', ip: str, **kwargs):
        #c104.set_debug_mode(c104.Debug.Point | c104.Debug.Server | c104.Debug.Client
        #                    | c104.Debug.Callback | c104.Debug.Connection | c104.Debug.Gil)

        port = kwargs.get('port', SERVER_DEFAULT_PORT)
        tick_rate_ms = kwargs.get('tick_rate_ms', SERVER_TICK_RATE_MS)
        tick_rate_ms = 50
        self.periodic_updates_ms = kwargs.get("periodic_update_ms", SERVER_UPDATE_PERIOD_MS)
        self.periodic_updates_start = kwargs.get("periodic_update_start", 0)
        if self.periodic_updates_start < 0:
            self.periodic_updates_start = self.periodic_updates_start % self.periodic_updates_ms

        self.server = c104.add_server(ip=ip, port=port, tick_rate_ms=tick_rate_ms)
        self.station = self.server.add_station(common_address=rtu.coa)

        self._periodic_update_points_queue = []
        self.periodic_updates_enable = kwargs.get("periodic_updates_enable", True)


        # station necessary for set_datapoints callback
        super().__init__(rtu, ip, **kwargs)
        contexts = {log_contexts.ON_SEND, log_contexts.ON_RECEIVE, log_contexts.PERIODIC,
                    log_contexts.DP_C}

        #self.logger.add_contexts(contexts)
        self.logger.setLevel(logging.INFO)

        # always pre-init dps for this Implementation
        if not kwargs.get('pre_init_datapoints'):
            self.set_datapoints()

    def _set_points_periodic(self):
        ref_time = self.rtu.manager.get_sim_start_time()
        offset = time.time() - ref_time
        self.logger.info(f"Simulation is running for {offset} seconds?")
        delay = self.periodic_updates_start - offset
        if delay > 0:
            self.logger.info(f"Delaying Periodic Updates by {delay}s")
            time.sleep(delay)
        else:
            self.logger.info(f"Periodic Updates Delay is smaller than 0 ({delay}s), skipping delay")

        for p in self._periodic_update_points_queue:
            self._set_periodic(p)
        self._periodic_update_points_queue = []

    def _set_periodic(self, point):
        if self.callbacks["on_before_auto_transmit"]:
            point.on_before_auto_transmit(callable=self._on_before_auto_transmit)
        point.report_ms = self.periodic_updates_ms

    def set_datapoints(self):
        self.points = {}
        for dp in self.data_points:
            info = dp["protocol_data"]
            coa = info["coa"]
            ioa = info["ioa"]
            type_id = info["type_id"]
            cot = info["cot"]
            if type_id < 45:
                point = self.station.add_point(
                    io_address=ioa,
                    type=c104.Type(type_id),
                    report_ms=0
                )
            else:
                point = self.station.add_point(
                    io_address=ioa,
                    type=c104.Type(type_id),
                    report_ms=0
                )

            if point is None:
                self.logger.error(f"Could not add Datapoint {ioa}")
                continue

            """
            self.points[ioa] = {
                "data": dp,
                "point": point
            }
            """
            self.points[ioa] = point

            if cot == COT.PERIODIC:
                if self.periodic_updates_enable:
                    if self.periodic_updates_start > 0:
                        self._periodic_update_points_queue.append(point)
                    else:
                        self._set_periodic(point)

            # Callbacks
            if int(point.type) in range(1, 45):
                if self.callbacks["on_before_read"]:
                    point.on_before_read(callable=self._on_before_read)
            elif int(point.type) in range(45, 70):
                if int(point.type) == 45 and self.callbacks["on_setpoint_command"] is not None:
                    point.on_receive(callable=self._on_setpoint_command)
                    self.rtu.logger.info("setting setpoint command")
                elif int(point.type) == 47 and self.callbacks['on_step_command'] is not None:
                    point.on_receive(callable=self._on_setpoint_command)
                    # keep that until I figured out how to properly handle
                elif self.callbacks["on_receive"]:
                    self.rtu.logger.info(f'adding rcv handler for IOA {ioa}')
                    point.on_receive(callable=self._on_receive)
                elif self.callbacks["on_setpoint_command"]:
                    point.on_receive(callable=self._on_setpoint_command)
                    self.rtu.logger.info(f"on_setpoint for non-45 point {coa}.{ioa}")

    def start(self):
        self._bind()
        threading.Thread(target=self._set_points_periodic).start()

    def _bind(self):
        if self.callbacks["on_send_apdu"] is not None:
            self.server.on_send_raw(callable=self._on_send_raw)
        if self.callbacks["on_receive_apdu"] is not None:
            self.server.on_receive_raw(callable=self._on_receive_raw)
        if self.callbacks["on_unexpected_msg"] is not None:
            self.server.on_unexpected_message(callable=self._on_unexpected_message)
        if self.callbacks["on_clock_synch"] is not None:
            self.server.on_clock_sync(callable=self._on_clock_sync)
        if self.callbacks["on_connect"] is not None:
            self.server.on_connect(callable=self._on_connect)
        self.server.start()

    def _on_clock_sync(self, server: c104.Server, ip: str, date_time: datetime.datetime) -> c104.ResponseState:
        self.callbacks['on_clock_synch'](date_time)
        return c104.ResponseState.SUCCESS

    def _on_connect(self, server: c104.Server, ip: str) -> bool:
        return self.callbacks['on_connect'](server, ip)

    def _on_send_raw(self, server: c104.Server, data: bytes) -> None:
        apdu = build_apdu_from_c104_bytes(data)
        self.callbacks["on_send_apdu"](apdu)

    def _on_receive_raw(self, server: c104.Server, data: bytes) -> None:
        apdu = build_apdu_from_c104_bytes(data)
        self.callbacks["on_receive_apdu"](apdu)

    def _on_unexpected_message(self, server: c104.Server, message: c104.IncomingMessage,
                               cause: c104.Umc) -> None:
        self.callbacks["on_unexpected_msg"](server, message, cause)

    def _on_receive(self, point: c104.Point, previous_state: dict,
                    message: c104.IncomingMessage) -> c104.ResponseState:
        prev_point = C104Point.parse_to_previous_point(previous_state, point)
        success = self.callbacks["on_receive"](C104Point(point), prev_point, message)
        return c104.ResponseState.SUCCESS if success else c104.ResponseState.FAILURE

    def _on_setpoint_command(self, point: c104.Point, previous_state: dict,
                             message: c104.IncomingMessage) -> c104.ResponseState:
        prev_point = C104Point.parse_to_previous_point(previous_state, point)
        success = self.callbacks["on_setpoint_command"](C104Point(point), prev_point, message)
        return c104.ResponseState.SUCCESS if success else c104.ResponseState.FAILURE

    def _on_step_command(self, point: c104.Point, previous_state: dict,
                         message: c104.IncomingMessage) -> bool:
        prev_point = C104Point.parse_to_previous_point(previous_state, point)
        return self.callbacks["on_step_command"](C104Point(point), prev_point, message)

    def _on_before_read(self, point: c104.Point) -> None:
        self.callbacks["on_before_read"](C104Point(point))

    def _on_before_auto_transmit(self, point: c104.Point) -> None:
        self.callbacks["on_before_auto_transmit"](C104Point(point))

    def on_reset_process(self):
        raise NotImplementedError()

    def _on_P_AC(self, *args, **kwargs):
        raise NotImplementedError()

    def on_C_CI(self):
        raise NotImplementedError()

    def _on_initial_C_CI(self):
        raise NotImplementedError()

    def _on_C_CI_request(self):
        raise NotImplementedError()

    def stop(self):
        #self.server.stop()
        pass
