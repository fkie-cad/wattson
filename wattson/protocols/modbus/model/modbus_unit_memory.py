import threading
from logging import Logger
from typing import Dict, Callable, Optional, Tuple, List

from pymodbus.constants import ExcCodes

from wattson.protocols.modbus.model.callbacks import ModbusOnValueReadCallback, ModbusOnValueWriteCallback, ModbusOnUnmappedReadCallback, \
    ModbusOnUnmappedWriteCallback, ModbusOnBeforeValueWriteCallback
from wattson.protocols.modbus.model.modbus_response_error import ModbusResponseError
from wattson.protocols.modbus.model.modbus_server_value_definition import ModbusServerValueDefinition
from wattson.protocols.modbus.model.modbus_table import ModbusTable
from wattson.protocols.modbus.model.modbus_value_definition import ModbusValueDefinition
from wattson.protocols.modbus.model.modbus_value_type import ModbusValueType
from wattson.util import get_logger


class ModbusUnitMemory:
    def __init__(self,
                 unit_id: int,
                 on_before_read_callback: Optional[ModbusOnValueReadCallback] = None,
                 on_before_write_callback: Optional[ModbusOnBeforeValueWriteCallback] = None,
                 on_write_callback: Optional[ModbusOnValueWriteCallback] = None,
                 on_unmapped_read_callback: Optional[ModbusOnUnmappedReadCallback] = None,
                 on_unmapped_write_callback: Optional[ModbusOnUnmappedWriteCallback] = None,
                 logger: Optional[Logger] = None):
        """

        Args:
            unit_id: The ID of the Unit this memory is for.
            on_before_read_callback: A callback called BEFORE a value is read. Read is if this callback returns True. Alternative: ExcCodes instance.
            on_before_write_callback: A callback called BEFORE a value is updated in memory. This should indicate whether the value can be written to (True).
            on_write_callback: A callback called AFTER a value is updated. The Expected return value is True.
                Other values will create a Modbus Exception Code response with potential side effects (other values might have been updated already)
            on_unmapped_read_callback: A callback called AFTER registers from an unmapped memory area are read.
            on_unmapped_write_callback: A callback called AFTER registers from an unmapped memory area are written.
            logger: A logger instance. If not provided, a default logger will be created.
        """
        self.unit_id = unit_id

        self._memory_lock: threading.RLock() = threading.RLock()

        self.on_before_read_callback = on_before_read_callback
        self.on_before_write_callback = on_before_write_callback

        self.on_write_callback = on_write_callback
        self.on_unmapped_read_callback = on_unmapped_read_callback
        self.on_unmapped_write_callback = on_unmapped_write_callback

        self.logger = logger
        if self.logger is None:
            self.logger = get_logger("ModbusUnitMemory")

        self.registers: Dict[int, int] = {}
        self.coils: Dict[int, bool] = {}
        self._table_offsets: Dict[ModbusTable, int] = {}

        self.values: Dict[str, ModbusValueDefinition] = {}
        self._coil_overlaps: Dict[int, ModbusValueDefinition] = {}
        self._register_overlaps: Dict[int, ModbusValueDefinition] = {}

    def clone(self) -> 'ModbusUnitMemory':
        """
        Creates a clone of this unit memory.
        This creates a deep copy of all register and coil values and creates copies of the ModbusValueDefinitions.
        Callbacks are **not** copied.
        """
        clone = ModbusUnitMemory(unit_id=self.unit_id, logger=self.logger)
        clone.registers = self.registers.copy()
        clone.coils = self.coils.copy()
        clone._table_offsets = self._table_offsets.copy()
        for value in self.values.values():
            cloned_value = value.clone()
            clone.register_value(cloned_value)
            if isinstance(cloned_value, ModbusServerValueDefinition):
                cloned_value.set_unit_memory(clone)
        return clone

    def _trigger_on_before_read(self, value_definition: ModbusValueDefinition) -> bool | ExcCodes:
        if callable(self.on_before_read_callback):
            return self.on_before_read_callback(value_definition)
        return True

    def _trigger_on_before_write(self, value_definition: ModbusValueDefinition, values: List[int] | List[bool]) -> bool | ExcCodes:
        if callable(self.on_before_write_callback):
            return self.on_before_write_callback(value_definition, values)
        return True

    def _trigger_on_write(self, value_definition: ModbusValueDefinition, values: List[int] | List[bool], value: ModbusValueType) -> bool | ExcCodes:
        if callable(self.on_write_callback):
            return self.on_write_callback(value_definition, values, value)
        return True

    def _trigger_on_unmapped_read(self, start_address: int, count: int, is_register: bool) -> bool | ExcCodes:
        if callable(self.on_unmapped_read_callback):
            return self.on_unmapped_read_callback(start_address, count, is_register)
        return True

    def _trigger_on_unmapped_write(self, start_address: int, values: List[int] | List[bool], is_register: bool) -> bool | ExcCodes:
        if callable(self.on_unmapped_write_callback):
            return self.on_unmapped_write_callback(start_address, values, is_register)
        return True

    def register_value(self, value_definition:  ModbusValueDefinition):
        self.values[value_definition.data_point_identifier] = value_definition
        for address in value_definition.get_addresses():
            if value_definition.is_register():
                self._register_overlaps[address] = value_definition
            else:
                self._coil_overlaps[address] = value_definition

    def get_value_definition_by_data_point_identifier(self, data_point_identifier: str) -> Optional[ModbusValueDefinition]:
        return self.values.get(data_point_identifier)

    def read_raw_values(self, start_address: int, count: int, register_type: ModbusTable) -> List[int] | List[bool]:
        """
        Reads from the specified address range and returns the register or coil values as a list.
        Args:
            start_address: The start address of the memory segment
            count: The number of registers or coils to read
            register_type: The type of register to read (COIL vs REGISTER)

        Returns:
            A list of (raw) register or coil values.
        """
        with self._memory_lock:
            # self.logger.info(f"Reading {count} {register_type.name} for address {start_address}")
            values = []
            for address in range(start_address, start_address + count):
                if register_type.is_register():
                    values.append(self.registers.get(address, 0))
                else:
                    values.append(self.coils.get(address, False))
            return values

    def write_raw_values(self, start_address: int, values: List[int] | List[bool], register_type: ModbusTable):
        """
        Writes the given values to the memory segment starting at start_address.
        Args:
            start_address: The start address to start writing to.
            values: The list of values to write, representing individual coils or registers
            register_type: The type of register to write to (COIL vs REGISTER)
        """
        with self._memory_lock:
            # self.logger.info(f"Writing {len(values)} to {register_type.name} for address {start_address}")
            for i, value in enumerate(values):
                if register_type.is_register():
                    if isinstance(value, int):
                        self.registers[start_address + i] = value & 0xFFFF
                    else:
                        raise ValueError(f"Cannot write {value} ({start_address} + {i}) to register {register_type.name} with {type(value)}")
                else:
                    if isinstance(value, bool):
                        self.coils[start_address + i] = value
                    elif isinstance(value, int) and value in [0, 1]:
                        self.coils[start_address + i] = bool(value)
                    else:
                        raise ValueError(f"Cannot write {value} ({start_address} + {i}) to coil {register_type.name} with {type(value)}")

    def _get_memory_map(self, start_address: int, count: int, register_type: ModbusTable) -> Dict[int, Tuple[Optional[ModbusValueDefinition], int, int]]:
        memory_map: Dict[int, Tuple[Optional[ModbusValueDefinition], int, int]] = {}

        affected = self._find_values_covering(start_address, count, is_register=register_type.is_register())

        # Unmapped areas: start_address mapped to count
        unmapped: Dict[int, int] = {}
        unmapped_list: List[int] = []
        # unmapped: List[Tuple[int, int]] = []
        last_unmapped: Optional[int] = None
        unmapped_leader: Optional[int] = None

        for address in range(start_address, start_address + count):
            if register_type.is_register():
                value_def = self._register_overlaps.get(address)
            else:
                value_def = self._coil_overlaps.get(address)
            if value_def is not None:
                continue

            unmapped_list.append(address)
            if unmapped_leader is not None and address == last_unmapped + 1:
                unmapped[unmapped_leader] += 1
                last_unmapped = address
            else:
                # New segment
                unmapped[address] = 1
                unmapped_leader = address
                last_unmapped = address

        for leader, count in unmapped.items():
            memory_map[leader] = (None, leader, count)
        for value_definition, overlap_start, overlap_count in affected:
            memory_map[overlap_start] = (value_definition, overlap_start, overlap_count)
        sorted_memory_map = {address: memory_map[address] for address in sorted(memory_map.keys())}
        return sorted_memory_map

    def _read_memory(self, start_address: int, count: int, register_type: ModbusTable) -> List[int] | List[bool]:
        memory_map = self._get_memory_map(start_address, count, register_type)
        # Pre-Read-Callbacks (called sequentially)
        for address, definition in memory_map.items():
            value_definition, value_address, value_count = definition
            try:
                if value_definition is None:
                    # Unmapped
                    read_allowed = self._trigger_on_unmapped_read(value_address, value_count, register_type.is_register())
                else:
                    read_allowed = self._trigger_on_before_read(value_definition)
                if not read_allowed:
                    read_allowed = ExcCodes.NEGATIVE_ACKNOWLEDGE
            except Exception as e:
                self.logger.error(f"Error in before read callback for {address}: {e}")
                read_allowed = ExcCodes.DEVICE_FAILURE
            if isinstance(read_allowed, ExcCodes):
                raise ModbusResponseError(read_allowed)
        # Actually read the memory
        return self.read_raw_values(start_address, count, register_type)

    def write_memory(self, start_address: int, values: List[int] | List[bool], register_type: ModbusTable) -> List[ModbusValueDefinition]:
        """
        Write the given values to the respective table memory.
        Usually, you should use the dedicated methods for this action!
        Args:
            start_address: The start address to start writing to.
            values: The list of values (registers or coils) to write
            register_type: The memory table (COIL, DISCRETE INPUT, HOLDING REGISTER, INPUT REGISTER) to write to.

        Returns:
            A list of affected ModbusValueDefinition objects.
        """
        return self._write_memory(start_address, values, register_type)

    def _write_memory(self, start_address: int, values: List[int] | List[bool], register_type: ModbusTable) -> List[ModbusValueDefinition]:
        memory_map = self._get_memory_map(start_address, len(values), register_type)
        sub_values_by_data_point: Dict[str, list] = {}
        affected_values: List[ModbusValueDefinition] = []
        for address, definition in memory_map.items():
            value_definition, value_address, value_count = definition
            try:
                if value_definition is None:
                    # Unmapped
                    relative_address = value_address - start_address
                    sub_values = values[relative_address:relative_address + value_count]
                    write_allowed = self._trigger_on_unmapped_write(value_address, sub_values, register_type.is_register())
                else:
                    affected_values.append(value_definition)
                    relative_address = value_address - start_address
                    relative_addresses = list(range(relative_address, relative_address + value_count))
                    sub_values = []
                    for a in relative_addresses:
                        if 0 <= a < len(values):
                            sub_values.append(values[a])
                        else:
                            sub_values.append(None)
                    sub_values_by_data_point[value_definition.data_point_identifier] = sub_values
                    write_allowed = self._trigger_on_before_write(value_definition, sub_values)
                if not write_allowed:
                    write_allowed = ExcCodes.NEGATIVE_ACKNOWLEDGE
            except Exception as e:
                self.logger.error(f"Error in before read callback for {address}: {e}")
                write_allowed = ExcCodes.DEVICE_FAILURE
            if isinstance(write_allowed, ExcCodes):
                raise ModbusResponseError(write_allowed)
        # Actually write the memory
        self.write_raw_values(start_address, values, register_type)
        # Notify written values
        for value_definition in affected_values:
            try:
                self._trigger_on_write(value_definition, sub_values_by_data_point[value_definition.data_point_identifier], value_definition.get_value())
            except Exception as e:
                self.logger.error(f"Error in write callback for {value_definition.data_point_identifier}: {e}")
        return affected_values

    def read_registers(self, start_address: int, count: int, register_type: ModbusTable = ModbusTable.INPUT_REGISTER) -> List[int]:
        """
        Returns a list of 16-bit integers for the registers starting at start_address.
        For all affected ModbusValueDefinitions, the on_before_read_callback is called,
        enabling a potential refresh of the register's contents.
        Args:
            start_address: The start address of the memory segment
            count: The number of registers to read
            register_type: The register type to read from (HOLDING REGISTER vs. INPUT REGISTER)

        Returns:
            A list of 16-bit integers for the registers
        Raises:
            ModbusResponseError if the read is rejected due to some (internal) error.
        """
        values = self._read_memory(start_address, count, register_type=register_type)
        # Sanity Check
        if len(values) != count:
            self.logger.error(f"Expected {count} registers, got {len(values)} ({values})")
            raise ModbusResponseError(ExcCodes.DEVICE_FAILURE)
        for value in values:
            if not isinstance(value, int):
                self.logger.error(f"Value {value} is not an integer")
                raise ModbusResponseError(ExcCodes.DEVICE_FAILURE)
        return values

    def write_registers(self, start_address: int, values: List[int], register_type: ModbusTable = ModbusTable.HOLDING_REGISTER) -> List[ModbusValueDefinition]:
        """
        Writes the given list of 16-bit register values into the registers.
        For each affected ModbusValueDefinition, the on_before_write_callback and the on_write_callback are called if applicable.
        Args:
            start_address: The start address of the memory segment
            values: The values to write
            register_type: The register type to write to (HOLDING REGISTER vs. INPUT REGISTER)

        Returns:
            The list of affected ModbusValueDefinitions.

        Raises:
            ModbusResponseError If the write is rejected or fails due to an (internal) error.
        """
        # Write to registers
        return self._write_memory(start_address, values, register_type=register_type)

    def read_coils(self, start_address: int, count: int, coil_type: ModbusTable = ModbusTable.COIL) -> List[bool]:
        """
        Reads all coil values from the memory segment starting at start_address of length count.
        Args:
            start_address: The start address of the memory segment
            count: The number of coils to read
            coil_type: The type of coil to read (COIL vs DISCRETE_INPUT)

        Returns:
            A list of coil values (bool)

        Raises:
            ModbusResponseError if the read is rejected due to some (internal) error.
        """
        self.logger.info(f"READ {count} COILS at {start_address}")
        values = self._read_memory(start_address, count, register_type=coil_type)
        # Sanity Check
        if len(values) != count:
            self.logger.error(f"Expected {count} coils, got {len(values)} ({values})")
            raise ModbusResponseError(ExcCodes.DEVICE_FAILURE)
        for value in values:
            if not isinstance(value, bool):
                self.logger.error(f"Value {value} is not a boolean")
                raise ModbusResponseError(ExcCodes.DEVICE_FAILURE)
        return values

    def write_coils(self, start_address: int, values: List[bool], coil_type: ModbusTable = ModbusTable.COIL) -> List[ModbusValueDefinition]:
        """
        Writes the given list of boolean register values into the target registers.
        For each affected ModbusValueDefinition, the on_before_write_callback and the on_write_callback are called if applicable.
        Args:
            start_address: The start address of the memory segment
            values: The values to write
            coil_type: Coil type to write to (COIL vs. DISCRETE INPUT)

        Returns:
            The list of affected ModbusValueDefinitions.

        Raises:
            ModbusResponseError If the write is rejected or fails due to an (internal) error.
        """
        # Write to registers
        return self._write_memory(start_address, values, register_type=coil_type)

    def _find_values_covering(self, start_address: int, count: int, is_register: bool) -> List[Tuple[ModbusValueDefinition, int, int]]:
        """
        For a given address range, returns ModbusValueDefinitions that are affected by this range.
        Args:
            start_address: The start address of the memory segment
            count: The length of the memory segment in coils or registers
            is_register: Whether the address refers to a register or a coil

        Returns:
            A list of (ModbusValueDefinition, address, count) tuples.

        """
        value_definitions = []
        found = []
        end_address = start_address + count - 1
        for address in range(start_address, start_address + count):
            if is_register:
                value_definition = self._register_overlaps.get(address, None)
            else:
                value_definition = self._coil_overlaps.get(address, None)
            if value_definition is None:
                continue
            if value_definition in value_definitions:
                continue
            value_definitions.append(value_definition)
            overlap = value_definition.get_overlap(start_address, end_address)
            if overlap is not None:
                overlap_start, overlap_count = overlap
                found.append((value_definition, overlap_start, overlap_count))
        return found

    def set_table_offset(self, modbus_table: ModbusTable, offset: int = 0, throw_on_mismatch: bool = True):
        current_offset = self._table_offsets.get(modbus_table)
        if current_offset is None:
            current_offset = offset
        self._table_offsets[modbus_table] = offset
        if offset != current_offset and throw_on_mismatch:
            raise ValueError(f"Modbus table offset for {modbus_table.name} has already been set to {current_offset} (!= {offset})")

    def get_table_offset(self, modbus_table: ModbusTable, default: Optional[int] = None) -> int:
        current_offset = self._table_offsets.get(modbus_table, default)
        if current_offset is None:
            raise ValueError(f"Modbus table offset for {modbus_table.name} has not been set")
        return current_offset

    def pdu_address_to_register_address(self, pdu_address: int, modbus_table: ModbusTable, zero_based_pdu: bool = True) -> int:
        zero_based_offset = 0 if zero_based_pdu else 1
        return pdu_address + self._table_offsets.get(modbus_table, 0) + zero_based_offset

    def register_address_to_pdu_address(self, register_address: int, modbus_table: ModbusTable, zero_based_pdu: bool = True) -> int:
        zero_based_offset = 0 if zero_based_pdu else 1
        return register_address - self._table_offsets.get(modbus_table, 0) - zero_based_offset
