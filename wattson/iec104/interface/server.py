from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional, Callable, Dict

from wattson.iec104.common.config import *
from wattson.iec104.common.datapoint import IEC104Point
from wattson.iec104.common.iec104message import IEC104Message

if TYPE_CHECKING:
    from wattson.hosts.rtu.rtu import RTU


class IECServerInterface(ABC):
    def __init__(self, rtu: 'RTU', ip: str, **kwargs):
        self.rtu = rtu
        self.logger = self.rtu.logger.getChild("IECSlave")
        self.ip = ip
        self.port = kwargs.get('port', SERVER_DEFAULT_PORT)
        self._tick_rate_ms = kwargs.get('tick_rate_ms', SERVER_TICK_RATE_MS)

        self.callbacks: dict[str, Optional[Callable]] = {
            "on_before_read": None,
            "on_before_auto_transmit": None,
            "on_send_apdu": None,
            "on_receive_apdu": None,
            "on_setpoint_command": None,
            "on_step_command": None,
            "on_unexpected_msg": None,
            "on_receive": None,
            "on_clock_synch": None,
            "on_connect": None
        }
        self.data_points = kwargs.get("datapoints", [])

        self.callbacks.update({key: val for (key, val) in kwargs.items() if key in self.callbacks})

        for c in ('on_receive_apdu', 'on_send_apdu'):
            if self.callbacks[c] is None:
                raise RuntimeError("Starting with on send/rcvd apdu as None-callback is invalid")

        self.points = {}
        self.mapped_point_info = {}
        # for use for C_CI (see 60870-5-5; 6.9.1), necessary for future compability - may need push upwards to RTU
        self.temporary_integrating_points = {}
        if kwargs.get('pre_init_datapoints', False):
            self.set_datapoints()

    @abstractmethod
    def set_datapoints(self):
        ...

    @abstractmethod
    def stop(self):
        ...

    @property
    def connection_string(self) -> str:
        return f"{self.ip}:{self.port}"

    def get_data_point_info(self, coa: int, ioa: int) -> Optional[Dict]:
        return self.mapped_point_info.get(coa, {}).get(ioa)

    def get_datapoint(self, ioa: int, update_value: bool = False) -> IEC104Point:
        p = self.points[ioa]
        if update_value:
            identifier = f"{p.coa}.{p.ioa}"
            p.value = self.rtu.manager.get_value(identifier)
        return p

    def has_datapoint(self, ioa: int) -> bool:
        return ioa in self.points

    @property
    def coa(self):
        return self.rtu.coa

    def on_reset_process(self):
        raise NotImplementedError()

    @abstractmethod
    def _on_P_AC(self, *args, **kwargs):
        """
        Parameter activation works by either: - loading >= 1 parameters to be activated in the future (P_ME) - loading + activating 1 parameter
        (P_AC) / activate loaded parameters (P_ME -> P_AC) See 60870-5-5 6.10
        Further details (60870-5-101, 7.4.9) P_AC is combined with ACT + DEACT and QPA (3rd bit) to act/deact periodic sending of an object P_ME
        + SPONT not valid in IEC101
        positive & negative ACT_CON need to send back the current, post-processing, parameter as value

        Args:
            *args:
                
            **kwargs:
                
        """

    def on_C_CI(self):
        """
        C_CI command requires more accurate handling as it is a command focussed on future requests See 60870-5-5 6.9.1

        """
        raise NotImplementedError()

    def _on_initial_C_CI(self):
        """
        Handles first command to from now on either memorize counter counters or memorize increments For increments, reset value to 0.
        Probably can forward regular counter-memorization to Pandapower-queries.
        See picture 92, 60870-5-101, p. 133

        """
        raise NotImplementedError()

    def _on_C_CI_request(self):
        """Handles later command that explicitly requests the current memory"""
        raise NotImplementedError()

    def _on_send_apdu(self, apdu):
        if self.callbacks["on_send_apdu"] is not None:
            self.callbacks["on_send_apdu"](apdu)

    def _on_receive_apdu(self, apdu):
        if self.callbacks["on_receive_apdu"] is not None:
            self.callbacks["on_receive_apdu"](apdu)

    def _on_unexpected_msg(self, server, message, cause):
        # TODO: Server != self?
        if self.callbacks["on_unexpected_msg"] is not None:
            self.callbacks["on_unexpected_msg"](self, message, cause)

    def _on_before_auto_transmit(self, point: IEC104Point):
        if self.callbacks["on_before_auto_transmit"] is not None:
            self.callbacks["on_before_auto_transmit"](point)

    def _on_before_read(self, point: IEC104Point):
        if self.callbacks["on_before_read"] is not None:
            self.callbacks["on_before_read"](point)

    def _on_setpoint_command(self, point: IEC104Point, prev_point: IEC104Point, msg: IEC104Message):
        if self.callbacks["on_setpoint_command"] is not None:
            self.callbacks["on_setpoint_command"](point, prev_point, msg)
