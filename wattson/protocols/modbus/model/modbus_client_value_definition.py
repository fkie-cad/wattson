import time
import warnings
from typing import Optional, List, TYPE_CHECKING, Callable

from wattson.protocols.modbus.model.modbus_endian import ModbusEndian
from wattson.protocols.modbus.model.modbus_table import ModbusTable
from wattson.protocols.modbus.model.modbus_value_definition import ModbusValueDefinition
from wattson.protocols.modbus.model.modbus_value_type import ModbusValueType

if TYPE_CHECKING:
    from wattson.protocols.modbus.model.modbus_unit_client import ModbusUnitClient


class ModbusClientValueDefinition(ModbusValueDefinition):
    def __init__(self,
                 data_point_identifier: str,
                 unit_id: int,
                 modbus_table: ModbusTable,
                 type_id: str,
                 register_width: int,
                 start_address: int,
                 endian: ModbusEndian,
                 address_is_zero_based: bool,
                 periodicity: Optional[float],
                 unit_client: 'ModbusUnitClient',
                 on_value_update_callback: Optional[Callable[['ModbusClientValueDefinition', Optional[ModbusValueType], Optional[ModbusValueType]], None]] = None
                 ):
        super(ModbusClientValueDefinition, self).__init__(
            data_point_identifier=data_point_identifier,
            unit_id=unit_id,
            modbus_table=modbus_table,
            type_id=type_id,
            register_width=register_width,
            start_address=start_address,
            endian=endian,
        )
        self._address_is_zero_based = address_is_zero_based
        self._cached_value: Optional[ModbusValueType] = None
        self._last_read_timestamp = -1
        self._last_write_timestamp = -1
        self.periodicity: Optional[float] = periodicity
        if self.periodicity is not None and self.periodicity <= 0:
            self.periodicity = None

        self.on_value_update_callback = on_value_update_callback
        self._unit_client = unit_client

    def clone(self) -> 'ModbusClientValueDefinition':
        return ModbusClientValueDefinition(
            data_point_identifier=self.data_point_identifier,
            unit_id=self.unit_id,
            modbus_table=self.modbus_table,
            type_id=self.type_id,
            register_width=self.register_width,
            start_address=self.start_address,
            endian=self.endian,
            address_is_zero_based=self._address_is_zero_based,
            periodicity=self.periodicity,
            unit_client=self._unit_client,
        )

    @property
    def unit_client(self) -> 'ModbusUnitClient':
        return self._unit_client

    def get_relative_address(self) -> int:
        return self.unit_client.get_relative_address(self)

    def get_zero_based_address(self) -> int:
        if self._address_is_zero_based:
            return self.start_address
        else:
            return self.start_address - 1

    def get_one_based_address(self) -> int:
        return self.get_zero_based_address() + 1

    def get_value(self) -> ModbusValueType | None:
        """
        Reads the locally cached value as a native value format (int, float, bool, str)
        Returns: The current value stored in the Modbus register(s) as a native data type or None
        """
        return self._cached_value

    def silent_set_value(self, value: ModbusValueType, silent: bool = True) -> bool:
        old_value = self._cached_value
        if not self._matches_value_type(type(value)):
            raise RuntimeError(f"Value {value} ({type(value)}) does not match {self.data_point_identifier} type {self.type_id}")
        if self.modbus_table.is_bool():
            if not isinstance(value, bool):
                raise RuntimeError(f"Can only write boolean values for {self.data_point_identifier} ({self.modbus_table.value})")
            self._cached_value = value
            if not silent:
                self._on_value_update(old_value, value)
            return True
        self._cached_value = value
        if not silent:
            self._on_value_update(old_value, value)
        return True

    def set_value(self, value: ModbusValueType) -> bool:
        """
        (Primarily intended for internal usage)
        Sets the locally cached value to the given value.
        The given value should be a native value type (int, float, bool, str) and must correspond to the configured type_id.
        This does NOT initialize a respective write request.
        Args:
            value: The value to set.

        Returns:
            True after the value has been set.
        Raises:
            RuntimeError: If the given value is not a native value type or an invalid value type has been passed.
        """
        return self.silent_set_value(value, silent=False)

    def _on_value_update(self, old_value: Optional[ModbusValueType], new_value: Optional[ModbusValueType]):
        if callable(self.on_value_update_callback):
            try:
                self.on_value_update_callback(self, old_value, new_value)
            except Exception as e:
                warnings.warn(f"Failed to call on_value_update_callback for {self.data_point_identifier}: {e}")

    def get_raw_value(self) -> List[int] | List[bool]:
        """
        Returns the raw values of the registers forming this value.
        This is a list of 16-bit integers, where list entry 0 corresponds to the register at this values start_address.
        Returns:
            A list of 16-bit integers representing the raw register values.
        """
        return self.encode_value(self._cached_value)

    def set_raw_value(self, values: List[int] | List[bool] | bool):
        """
        Sets the value based on the raw modbus register values.
        This does not set the register values but parses the given register representation into the native value.
        Args:
            values: The raw values to set.
        """
        if isinstance(values, bool):
            values = [values]
        if len(values) != self.register_width:
            raise ValueError(f"Expected {len(values)} values, got {len(values)} for ModbusValueDefinition {self.data_point_identifier}")
        self.set_value(self.decode_value(values))

    def read_raw(self, update_cache: bool = True, global_notify_read_done: bool = True) -> List[int] | List[bool]:
        """
        Reads the registers forming this value from the server.
        Returns:
            A list of 16-bit integers or booleans representing the raw register values.
        """
        values = self.unit_client.device_client.modbus_client.read_raw_data_point(
            self.data_point_identifier, error_as_none=False, global_notify_read_done=global_notify_read_done
        )
        if update_cache:
            self._last_read_timestamp = time.time()
            self.set_value(self.decode_value(values))
        return values

    def read(self, global_notify_read_done: bool = True) -> ModbusValueType:
        """
        Reads the value from the server.
        Returns:
            The respective value as a native value type.
        """
        self.read_raw(update_cache=True, global_notify_read_done=global_notify_read_done)
        return self._cached_value

    def async_read(self,
                   callback: Optional[Callable[['ModbusValueDefinition', bool, Optional[int], List[int] | List[bool], ModbusValueType], None]] = None,
                   global_notify_read_done: bool = True) -> bool:
        """
        Reads the registers forming this value from the server asynchronously.
        As soon as the response arrives, the callback function will be called
        with the instance of this ValueDefinition, a bool indicating whether the value was successfully read or not,
        the register values and the resulting native value.
        Args:
            callback: The callback to call when reading from the server.
            global_notify_read_done: Whether to call the global callback when the process is done.
        Returns:
            True if the write request has been accepted by the client.
        """
        return self._unit_client.device_client.modbus_client.read_data_point_callback(
            self.data_point_identifier, callback, True, global_notify_read_done=global_notify_read_done
        )

    def write_raw(self, values: Optional[List[int] | List[bool]] = None) -> bool:
        """
        Writes the raw register values to the server.
        Args:
            values: The raw values to set. If not given, the currently cached value is used.
        Raises:
            ValueError: If the value representation does not match this value definition
        Returns:
            True iff the write request was successful.
        """
        if values is None:
            values = self.encode_value(self._cached_value)
        if len(values) != self.register_width:
            raise ValueError("The given values do not match the expected register width")
        return self._unit_client.device_client.modbus_client.write_raw_data_point(self.data_point_identifier, values)

    def write(self, value: Optional[ModbusValueType] = None) -> bool:
        """
        Writes the value to the server's register(s).
        Args:
            value: If given, the internal value is set to this value before writing to the server.
        Raises:
            ValueError: If the value representation does not match this value definition or the local value is None.
        Returns:
            Whether the write was successful.
        """
        if value is not None:
            self._cached_value = value
        if self._cached_value is None:
            raise ValueError("Cannot write None")
        return self.write_raw()

    def async_write(self,
                    value: Optional[ModbusValueType] = None,
                    callback: Optional[Callable[['ModbusValueDefinition', bool, Optional[int], List[int] | List[bool], ModbusValueType], None]] = None
                    ) -> bool:
        """
        Writes the value to the server's register(s) asynchronously.
        As soon as the response arrives or an error occurs, the callback function will be called.
        Args:
            value: If given, the internal value is set to this value before writing to the server.
            callback: The callback to call when reading from the server. The instance of this ValueDefinition is passed along with a success indicator.
        Returns:
            True if the client has accepted the write.
        """
        return self._unit_client.device_client.modbus_client.write_data_point_callback(self.data_point_identifier, value, callback, error_as_false=True)
