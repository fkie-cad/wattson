import copy
import re
from typing import Optional, Any, Callable
import pytimeparse2

from wattson.cosimulation.control.interface.wattson_client import WattsonClient
from wattson.cosimulation.remote.wattson_remote_object import WattsonRemoteObject
from wattson.cosimulation.remote.wattson_remote_representation import WattsonRemoteRepresentation


class NetworkLinkModel(WattsonRemoteRepresentation, WattsonRemoteObject):
    def synchronize(self, force: bool = False, block: bool = True):
        raise NotImplementedError("")

    def set_on_change_callback(self, callback: Callable[[str, Any], None]):
        """
        Sets a callback to be called when a value is updated.
        The callback is called with the parameter name and the new value.
        @param callback: The callback to call
        @return:
        """
        self["on_change_callback"] = callback

    def on_change(self, parameter: str, value: Any):
        callback = self.get("on_change_callback")
        if callback is not None:
            callback(parameter, value)

    @property
    def is_remote(self) -> bool:
        return self.get("is_remote", False)

    @property
    def wattson_client(self) -> Optional['WattsonClient']:
        return self.get("wattson_client")

    @property
    def delay_ms(self) -> Optional[int]:
        return self.get("delay_ms")

    @delay_ms.setter
    def delay_ms(self, delay: int):
        self["delay_ms"] = delay
        self.on_change("delay_ms", delay)

    def set_delay_from_timespan(self, timespan: str):
        """
        Sets the delay from a timespan textual representation. E.g., "1.5s" becomes a delay of 1500 ms
        @param timespan: The timespan string to parse
        """
        ts = pytimeparse2.parse(timespan)
        if ts is None:
            ts = 0
        self.delay_ms = int(ts * 1000)

    @property
    def jitter_ms(self) -> Optional[int]:
        return self.get("jitter_ms")

    @jitter_ms.setter
    def jitter_ms(self, jitter: int):
        self["jitter_ms"] = jitter
        self.on_change("jitter_ms", jitter)

    def set_jitter_from_timespan(self, timespan: str):
        """
        Sets the jitter from a timespan textual representation. E.g., "1.5s" becomes a jitter of 1500 ms
        @param timespan: The timespan string to parse
        """
        ts = pytimeparse2.parse(timespan)
        if ts is None:
            ts = 0
        self.jitter_ms = int(ts * 1000)

    @property
    def bandwidth_bits_per_second(self) -> Optional[int]:
        return self.get("bandwidth_bits_per_second")

    @property
    def bandwidth_mbps(self) -> Optional[float]:
        bw = self.bandwidth_bits_per_second
        if bw is None:
            return None
        return bw / 1000**2

    @bandwidth_bits_per_second.setter
    def bandwidth_bits_per_second(self, bandwidth: int):
        self["bandwidth_bits_per_second"] = bandwidth
        self.on_change("bandwidth_bits_per_second", bandwidth)

    def set_bandwidth_from_string(self, bandwidth_string: str):
        if len(bandwidth_string) == 0:
            # Default: Set to 1GBit
            self.bandwidth_bits_per_second = 1000**3
            return
        self.bandwidth_bits_per_second = self._parse_bandwidth_string(bandwidth_string)

    @property
    def packet_loss_percent(self) -> Optional[float]:
        return self.get("packet_loss_percent")

    @packet_loss_percent.setter
    def packet_loss_percent(self, packet_loss: float):
        self["packet_loss_percent"] = packet_loss
        self.on_change("packet_loss_percent", packet_loss)

    def set_packet_loss_from_string(self, packet_loss_string: str):
        if len(packet_loss_string) == 0:
            self.packet_loss_percent = 0
            return
        self.packet_loss_percent = float(packet_loss_string.replace("%", ""))

    @property
    def bit_error_rate(self) -> Optional[float]:
        return self.get("bit_error_rate")

    @bit_error_rate.setter
    def bit_error_rate(self, bit_error_rate: float):
        self["bit_error_rate"] = bit_error_rate
        self.on_change("bit_error_rate", bit_error_rate)

    def set_bit_error_rate_from_string(self, bit_error_rate_string: str):
        if len(bit_error_rate_string) == 0:
            self.bit_error_rate = 0
            return
        self.bit_error_rate = float(bit_error_rate_string.replace("%", ""))

    def to_remote_representation(self, force_state_synchronization: bool = True) -> 'NetworkLinkModel':
        properties = ["delay_ms", "jitter_ms", "bandwidth_bits_per_second", "packet_loss_percent", "bit_error_rate"]
        return NetworkLinkModel({p: self.get(p) for p in properties})

    def equals(self, other: 'NetworkLinkModel'):
        other_repr = other.to_remote_representation()
        for key, value in self.to_remote_representation().items():
            if other_repr[key] != value:
                return False
        return True

    def to_wattson_remote_object(self, wattson_client: 'WattsonClient') -> WattsonRemoteObject:
        self["is_remote"] = True
        self["wattson_client"] = wattson_client
        return self

    @staticmethod
    def _parse_bandwidth_string(bandwidth: str) -> int:
        """
        Converts a bandwidth given in Bps, Kbps, Mbps or Gpbs (with unit) to
        bps without unit

        @param bandwidth:
        @return:
        """
        # Regex for multiple digits followed by multiple letters (e.g., 123Mbps)
        match = re.match(r"([0-9]+)([a-z]+)", bandwidth, re.I)
        if not match:
            raise RuntimeError(f"Invalid Bandwidth: {bandwidth}")

        items = match.groups()
        assert len(items) == 2
        val = int(items[0])
        unit = items[1].lower()
        if unit not in ["bps", "kbps", "mbps", "gbps"]:
            raise RuntimeError(f"Invalid bandwidth unit: {unit}")
        scale = {
            "bps": 1,
            "kbps": 1000,
            "mbps": 1000**2,
            "gbps": 1000**3
        }
        return int(val * scale[unit])
