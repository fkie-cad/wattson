from typing import Optional, Set

from wattson.iec104.interface.types import QualityBit


class QualityByte:
    def __init__(self, bits: Optional[Set['QualityBit']] = None):
        self.bits = bits if bits else set()
        self._invalid = QualityBit.INVALID in self.bits
        self._non_topical = QualityBit.NON_TOPICAL in self.bits
        self._substituded = QualityBit.SUBSTITUTED in self.bits
        self._blocked = QualityBit.BLOCKED in self.bits
        self._elapsed_time_invalid = QualityBit.ELAPSED_TIME_INVALID in self.bits
        self._overflow = QualityBit.OVERFLOW in self.bits

    def __str__(self):
        return hex(self.value)

    def __repr__(self):
        return str(self)

    @property
    def value(self):
        return sum(self.bits)

    @property
    def is_invalid(self):
        return self._invalid

    @property
    def is_non_topical(self):
        return self._non_topical

    @property
    def is_substituded(self):
        return self._substituded

    @property
    def is_blocked(self):
        return self._blocked

    @property
    def has_invalid_elapsed_time(self):
        return self._elapsed_time_invalid

    @property
    def is_overflow(self):
        return self._overflow

    @property
    def is_good(self):
        return self.value == 0

    @property
    def is_valid_value(self):
        """
        assumes that ONLY a substituded value keeps the packet as generally accepted,
        whereas all others require handling similar to an invalid value
        """
        return self.value in (0, QualityBit.SUBSTITUTED)
