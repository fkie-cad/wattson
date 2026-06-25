import asyncio
import logging
import threading
from logging import Logger
from typing import Dict, Optional, List, Any, Callable, Tuple

from pymodbus.constants import ExcCodes
from pymodbus.datastore.context import ModbusDeviceContext, ModbusServerContext
from pymodbus import ModbusDeviceIdentification
from pymodbus.pdu import ModbusPDU
from pymodbus.server import ServerStop, ModbusTcpServer

from powerowl.layers.network.configuration.protocols.protocol_name import ProtocolName
from wattson.protocols.modbus.model.modbus_data_block import ModbusDataBlock
from wattson.protocols.modbus.model.modbus_endian import ModbusEndian
from wattson.protocols.modbus.model.modbus_server_value_definition import ModbusServerValueDefinition
from wattson.protocols.modbus.model.modbus_table import ModbusTable
from wattson.protocols.modbus.model.modbus_unit_memory import ModbusUnitMemory
from wattson.protocols.modbus.model.modbus_value_definition import ModbusValueDefinition
from wattson.protocols.modbus.model.modbus_value_type import ModbusValueType
from wattson.util import get_logger
from wattson.util.threading import set_thread_name


class ModbusServer(threading.Thread):
    def __init__(self,
                 bind_ip: str = "",
                 bind_port: int = 502,
                 device_id: str = None,
                 zero_based: bool = True,
                 logger: Optional[Logger] = None
                 ):
        super(ModbusServer, self).__init__()

        self.bind_ip = bind_ip
        self.bind_port = bind_port
        self.device_id = device_id
        self.units: Dict[int, ModbusDeviceContext] = {}
        self.zero_based = zero_based
        self._zero_based_data_point: Optional[bool] = None

        self._on_client_connect = None
        self._on_client_disconnect = None
        self._on_receive_pdu = None
        self._on_sent_pdu = None
        self._on_before_read = None
        self._on_before_write = None
        self._on_value_write = None
        self._on_unmapped_write = None
        self._on_unmapped_read = None

        self.data_point_dict: Dict[str, dict] = {}
        self.data_points_by_unit: Dict[int, Dict[str, dict]] = {}
        # DataPoint ID to value definition
        self._value_definitions: Dict[str, ModbusServerValueDefinition] = {}

        self.unit_memories: Dict[int, ModbusUnitMemory] = {}

        self._known_connections: Dict[str, Tuple[str, int]] = {}
        self._connection_id_by_peer_info: Dict[Tuple[str, int], str] = {}

        self.logger = logger
        if self.logger is None:
            self.logger = get_logger("ModbusServer")
            self.logger.setLevel(logging.DEBUG)

        self.server_context: Optional[ModbusServerContext] = None
        self.server_identity: Optional[ModbusDeviceIdentification] = None
        self.pymodbus_server: Optional[ModbusTcpServer] = None
        self.coroutine: Optional[asyncio.Future] = None
        self._shutdown_requested = threading.Event()

    def set_on_client_connect(self, on_client_connect: Callable[[str, int], bool]):
        self._on_client_connect = on_client_connect

    def set_on_client_disconnect(self, on_client_disconnect: Callable[[str, int], None]):
        self._on_client_disconnect = on_client_disconnect

    def set_on_receive_pdu(self, on_receive_pdu: Callable[[str, int, ModbusPDU], None]):
        self._on_receive_pdu = on_receive_pdu

    def set_on_sent_pdu(self, on_sent_pdu: Callable[[str, int, ModbusPDU], None]):
        self._on_sent_pdu = on_sent_pdu

    def set_on_before_read(self, on_before_read: Callable[[ModbusServerValueDefinition], bool | ExcCodes]):
        self._on_before_read = on_before_read

    def set_on_before_write(self, on_before_write: Callable[[ModbusValueDefinition, List[int] | List[bool]], bool | ExcCodes]):
        self._on_before_write = on_before_write

    def set_on_value_write(self, on_value_write: Callable[[ModbusValueDefinition, List[int] | List[bool], ModbusValueType], bool | ExcCodes]):
        self._on_value_write = on_value_write

    def get_unit_list(self) -> List[int]:
        return list(self.units.keys())

    def get_unit_memory(self, unit_id: int) -> ModbusUnitMemory:
        return self.unit_memories[unit_id]

    def set_value(self, data_point_identifier: str, value: Any):
        value_def = self._value_definitions.get(data_point_identifier)
        if value_def is None:
            raise RuntimeError(f"No ModbusValueDefinition for {data_point_identifier}")
        value_def.set_value(value)

    def get_value(self, data_point_identifier: str) -> Any:
        value_def = self._value_definitions.get(data_point_identifier)
        if value_def is None:
            raise RuntimeError(f"No ModbusValueDefinition for {data_point_identifier}")
        return value_def.get_value()

    def get_value_definition(self, data_point_identifier: str) -> Optional[ModbusServerValueDefinition]:
        return self._value_definitions.get(data_point_identifier)

    def start(self):
        # Create Slaves / Devices
        for unit_id, memory in self.unit_memories.items():
            discrete_block = ModbusDataBlock(
                memory,
                ModbusTable.DISCRETE_INPUT,
                use_zero_based_addressing=self.zero_based,
                address_offset=memory.get_table_offset(ModbusTable.DISCRETE_INPUT, 0),
            )
            coils_block = ModbusDataBlock(
                memory,
                ModbusTable.COIL,
                use_zero_based_addressing=self.zero_based,
                address_offset=memory.get_table_offset(ModbusTable.COIL, 0),
            )
            input_block = ModbusDataBlock(
                memory,
                ModbusTable.INPUT_REGISTER,
                use_zero_based_addressing=self.zero_based,
                address_offset=memory.get_table_offset(ModbusTable.INPUT_REGISTER, 0),
            )
            holding_block = ModbusDataBlock(
                memory,
                ModbusTable.HOLDING_REGISTER,
                use_zero_based_addressing=self.zero_based,
                address_offset=memory.get_table_offset(ModbusTable.HOLDING_REGISTER, 0),
            )
            unit_context = ModbusDeviceContext(di=discrete_block, co=coils_block, hr=holding_block, ir=input_block)
            self.units[unit_id] = unit_context

        self.server_context = ModbusServerContext(self.units, single=False)
        self.server_identity = ModbusDeviceIdentification(
            info_name={
                "VendorName": "Wattson",
                "ProductCode": "WattsonModbus",
                "VendorUrl": "https://wattson.it",
                "ProductName": "Wattson Modbus Server",
                "ModelName": "Wattson RTU",
                "MajorMinorRevision": "1.0"
            }
        )
        super().start()

    def run(self):
        set_thread_name(f"W/MB/S/Main")
        asyncio.run(self.run_async_server())

    async def run_async_server(self):
        self.logger.info("Starting Asynchronous Modbus Server")
        address = (self.bind_ip, self.bind_port)
        self.logger.info(f"Starting on {address}")
        self.pymodbus_server = ModbusTcpServer(
            context=self.server_context,
            identity=self.server_identity,
            address=address,
            trace_connect=self._pymodbus_on_client_connection_change,
            trace_pdu=self._pymodbus_on_pdu,
            trace_packet=self._pymodbus_on_packet
        )
        await self.pymodbus_server.serve_forever()
        self.logger.info(f"Server stopped")

    def stop(self):
        self._shutdown_requested.set()
        try:
            ServerStop()
        except RuntimeError:
            pass

    def set_data_points(self, data_points: list):
        """
        Set the data points that should be handled by this server.
        For each data point, the respective unit is created and the registers are reserved.
        Args:
            data_points: A list of data points (each data point is a dict)
        """
        for data_point in data_points:
            if data_point.get("protocol") != ProtocolName.MODBUS_TCP.value:
                self.logger.warning(f"DataPoint with protocol {data_point.get('protocol')} is not supported by ModbusServer")
                continue
            protocol_data = data_point.get("protocol_data")
            unit_id = int(protocol_data.get("unit_id"))
            identifier = data_point.get("identifier")
            self.data_point_dict[identifier] = data_point
            self.data_points_by_unit.setdefault(unit_id, {})[identifier] = data_point
            if unit_id not in self.unit_memories:
                self.unit_memories[unit_id] = ModbusUnitMemory(
                    unit_id,
                    on_before_read_callback=self._trigger_on_before_read,
                    on_before_write_callback=self._trigger_on_before_write,
                    on_write_callback=self._trigger_on_write,
                    on_unmapped_write_callback=self._trigger_on_unmapped_write,
                    on_unmapped_read_callback=self._trigger_on_unmapped_read
                )
            modbus_table = ModbusTable(protocol_data.get("table"))
            table_offset = protocol_data.get("table_offset", 0)
            zero_based = protocol_data.get("zero_based_address", True)
            if self._zero_based_data_point is not None and self._zero_based_data_point != zero_based:
                raise ValueError("Inconsistent addressing scheme - some data points state zero-based addresses while others don't")
            self._zero_based_data_point = zero_based
            self.zero_based = self._zero_based_data_point

            self.unit_memories[unit_id].set_table_offset(modbus_table, table_offset, throw_on_mismatch=True)
            # Validate PDU address if present
            if "zero_based_pdu_address" in protocol_data:
                pdu_address = protocol_data.get("zero_based_pdu_address")
                calculated_register = self.unit_memories[unit_id].pdu_address_to_register_address(pdu_address, modbus_table, zero_based)
                if calculated_register != protocol_data.get("address"):
                    raise RuntimeError(f"Expected absolute register address {protocol_data['address']}, calculated {calculated_register} instead.")

            value_definition = ModbusServerValueDefinition(
                identifier,
                unit_id,
                modbus_table=modbus_table,
                type_id=protocol_data.get("type_id"),
                register_width=protocol_data.get("width"),
                start_address=protocol_data.get("address"),
                endian=ModbusEndian(protocol_data.get("endian")),
                unit_memory=self.unit_memories[unit_id]
            )

            self._value_definitions[identifier] = value_definition
            self.unit_memories[unit_id].register_value(value_definition)
        self.logger.info(f"Registered {len(self._value_definitions)} data points across {len(self.unit_memories)} units")

    def _trigger_on_before_read(self, value_definition: ModbusValueDefinition) -> bool | ExcCodes:
        self.logger.debug(f"Read request for {value_definition.data_point_identifier} - updating memory")
        if callable(self._on_before_read):
            ret = self._on_before_read(value_definition)
            if isinstance(ret, ExcCodes):
                return ret
            return bool(ret)
        return True

    def _trigger_on_before_write(self, value_definition: ModbusValueDefinition, registers_or_coils: List[Optional[int]] | List[Optional[bool]]) -> bool | ExcCodes:
        self.logger.debug(f"Write request for {value_definition.data_point_identifier}")
        self.logger.info(f"Write request for {value_definition.data_point_identifier}")
        ret = True
        if callable(self._on_before_write):
            try:
                ret = self._on_before_write(value_definition, registers_or_coils)
                if not isinstance(ret, ExcCodes):
                    ret = bool(ret)
            except Exception as e:
                self.logger.error(f"Error in on_before_write for {value_definition.data_point_identifier} - {e}")
                ret = ExcCodes.DEVICE_FAILURE
        return ret

    def _trigger_on_write(self, value_definition: ModbusValueDefinition, registers_or_coils: List[int] | List[bool], new_value: ModbusValueType) -> bool | ExcCodes:
        self.logger.debug(f"Writing to {value_definition.data_point_identifier}: {new_value} ({type(new_value)})")
        self.logger.info(f"Writing to {value_definition.data_point_identifier}: {new_value} ({type(new_value)})")
        ret = True
        if callable(self._on_value_write):
            try:
                ret = self._on_value_write(value_definition, registers_or_coils, new_value)
                if not isinstance(ret, ExcCodes):
                    ret = bool(ret)
            except Exception as e:
                self.logger.error(f"Failed in on_value_write callback for {value_definition.data_point_identifier}: {e}")
                ret = False
        return ret

    def _trigger_on_unmapped_write(self, address: int, values: List[int] | List[bool], is_register: bool) -> bool | ExcCodes:
        memory_type = "Register" if is_register else "Coil"
        self.logger.debug(f"Unmapped write to {memory_type} - {address}: {values}")
        if callable(self._on_unmapped_write):
            try:
                ret = self._on_unmapped_write(address, values, is_register)
                if not isinstance(ret, ExcCodes):
                    ret = bool(ret)
                return ret
            except Exception as e:
                self.logger.error(f"Failed in on_unmapped_write callback for {address}: {e}")
                return ExcCodes.DEVICE_FAILURE
        return True

    def _trigger_on_unmapped_read(self, address: int, count: int, is_register: bool) -> bool | ExcCodes:
        memory_type = "Register" if is_register else "Coil"
        self.logger.debug(f"Unmapped reading {count} {memory_type}(s) at {address}")
        if callable(self._on_unmapped_read):
            try:
                ret = self._on_unmapped_read(address, count, is_register)
                if not isinstance(ret, ExcCodes):
                    ret = bool(ret)
                return ret
            except Exception as e:
                self.logger.error(f"Failed in on_unmapped_read callback for {address}: {e}")
                return ExcCodes.DEVICE_FAILURE
        return True

    """
    PyModbus Callbacks
    """
    def _pymodbus_on_client_connection_change(self, connect: bool):
        self.logger.debug(f"PyModbusOnClientConnect: {connect=}")
        host = ""
        port = -1
        connection_found: bool = False
        if connect:
            for cid, connection in self.pymodbus_server.active_connections.items():
                if cid not in self._known_connections:
                    connection_found = True
                    self._known_connections[cid] = connection.transport.get_extra_info("peername", (host, port))
                    host, port = self._known_connections[cid]
                    self._connection_id_by_peer_info[(host, port)] = cid
                    self.logger.debug(f"Connected ID: {cid} ({host}:{port})")
                    break
            if not connection_found:
                self.logger.warning(f"Could not find new connection!")
            if callable(self._on_client_connect):
                ret = self._on_client_connect(host, port)
                if not ret:
                    cid = self._connection_id_by_peer_info[(host, port)]
                    self.logger.debug(f"Closing connection {host}:{port} ({cid}) due to user rejection")
                    self._known_connections.pop(cid)
                    self._connection_id_by_peer_info.pop((host, port))
                    self.pymodbus_server.active_connections[cid].close()
        else:
            found: bool = False
            for cid in self._known_connections:
                if cid not in self.pymodbus_server.active_connections:
                    host, port = self._known_connections[cid]
                    self._known_connections.pop(cid)
                    self._connection_id_by_peer_info.pop((host, port))
                    found = True
                    break
            if found and callable(self._on_client_disconnect):
                self._on_client_disconnect(host, port)

    def _pymodbus_on_pdu(self, sent: bool, pdu: ModbusPDU) -> ModbusPDU:
        if sent:
            self.logger.debug(f"PyModbusOnPduSent: {pdu=}")
            if callable(self._on_sent_pdu):
                try:
                    self._on_sent_pdu("???", -1, pdu)
                except Exception as e:
                    self.logger.error(f"Error in on_sent_pdu callback: {e}")
        else:
            self.logger.debug(f"PyModbusOnPduReceived: {pdu=}")
            if callable(self._on_receive_pdu):
                try:
                    self._on_receive_pdu("???", -1, pdu)
                except Exception as e:
                    self.logger.error(f"Error in on_receive_pdu callback: {e}")
        return pdu

    def _pymodbus_on_packet(self, sent: bool, data: bytes) -> bytes:
        if sent:
            self.logger.debug(f"PyModbusOnPacketSent: {data=}")
        else:
            self.logger.debug(f"PyModbusOnPacketReceived: {data=}")
        return data
