import asyncio
import concurrent
import threading
import time
from concurrent.futures import ThreadPoolExecutor, Future
from logging import Logger, exception
from typing import Optional, Dict, Callable, List, Awaitable, Tuple

import pymodbus.logging
from pymodbus.client import ModbusBaseClient as PyModbusBaseClient
from pymodbus.client import AsyncModbusTcpClient as AsyncPyModbusTcpClient
from pymodbus.constants import ExcCodes
from pymodbus.pdu import ModbusPDU

from powerowl.layers.network.configuration.protocols.protocol_name import ProtocolName
from wattson.protocols.modbus.model.modbus_client_value_definition import ModbusClientValueDefinition
from wattson.protocols.modbus.model.modbus_device_client import ModbusDeviceClient
from wattson.protocols.modbus.model.modbus_endian import ModbusEndian
from wattson.protocols.modbus.model.modbus_error import ModbusError
from wattson.protocols.modbus.model.modbus_response_error import ModbusResponseError
from wattson.protocols.modbus.model.modbus_table import ModbusTable
from wattson.protocols.modbus.model.modbus_unit_client import ModbusUnitClient
from wattson.protocols.modbus.model.modbus_value_definition import ModbusValueDefinition
from wattson.protocols.modbus.model.modbus_value_type import ModbusValueType
from wattson.protocols.modbus.util.periodic_task import PeriodicTask
from wattson.util import get_logger
from wattson.util.threading import set_thread_name


class ModbusClient(threading.Thread):
    """
    The ModbusClient allows managing multiple Modbus connections via TCP or TLS.
    For each connection, i.e., device, a dedicated PyModbusClient instance is created and managed.
    Each device can manage different units.
    - polling of Modbus registers and coils
    - Abstracted read and write operations for data points
    - Callbacks
    - Forwarding of protocol data
    - Zero-based and one-based addressing for each Unit (defaults to zero-based-addressing)
    - Object-oriented data point wrappers (ModbusValueDefinition)
    - Object-oriented Data Unit Clients (ModbusUnitClient)
    """
    def __init__(self, server_data: Dict[str, dict], logger: Optional[Logger] = None):
        super(ModbusClient, self).__init__()
        self.logger = logger
        if self.logger is None:
            self.logger = get_logger("ModbusClient")

        self._server_data = server_data

        # Devices and units by their respective IDs. Each device is a server with one or multiple units.
        self._servers: Dict[str, ModbusDeviceClient] = {}
        # Data points by identifier
        self._data_points: Dict[str, dict] = {}
        self._value_definition_by_data_point: Dict[str, ModbusClientValueDefinition] = {}

        # Key: Server ID
        self._pymodbus_clients: Dict[str, PyModbusBaseClient] = {}
        # Key: Server ID; Each thread handles a single connection
        self._client_threads: Dict[str, threading.Thread] = {}
        self._client_loops: Dict[str, asyncio.AbstractEventLoop] = {}
        self._periodic_loops: List[Tuple[asyncio.AbstractEventLoop, asyncio.Event]] = []
        self._client_periodic_threads: Dict[str, threading.Thread] = {}
        self._connected_events: Dict[str, threading.Event] = {}

        self._stop_requested = threading.Event()
        # High level callbacks
        self._on_read_callback: Optional[Callable[[ModbusClientValueDefinition, bool, Optional[int], List[int] | List[bool], ModbusValueType], None]] = None
        self._on_write_callback: Optional[Callable[[ModbusClientValueDefinition, bool, Optional[int], List[int] | List[bool], ModbusValueType], None]] = None
        self._on_value_update_callback: Optional[Callable[[ModbusClientValueDefinition, Optional[ModbusValueType], Optional[ModbusValueType]], None]] = None
        # Protocol level callbacks
        self._on_send_pdu: Optional[Callable[[ModbusDeviceClient, ModbusPDU], None]] = None
        self._on_receive_pdu: Optional[Callable[[ModbusDeviceClient, ModbusPDU], None]] = None
        # Connection callbacks
        ## device_client; connected; ip; port
        self._on_connection_change_callback: Optional[Callable[[ModbusDeviceClient, bool, str, int], None]] = None

    def set_on_read_callback(self, callback: Callable[[ModbusClientValueDefinition, bool, Optional[int], List[int] | List[bool], ModbusValueType], None]):
        self._on_read_callback = callback

    def set_on_write_callback(self, callback: Callable[[ModbusClientValueDefinition, bool, Optional[int], List[int] | List[bool], ModbusValueType], None]):
        self._on_write_callback = callback

    def set_on_value_update_callback(self, callback: Callable[[ModbusClientValueDefinition, Optional[ModbusValueType], Optional[ModbusValueType]], None]):
        self._on_value_update_callback = callback

    def set_on_send_pdu_callback(self, callback: Callable[[ModbusDeviceClient, ModbusPDU], None]):
        self._on_send_pdu = callback

    def set_on_receive_pdu_callback(self, callback: Callable[[ModbusDeviceClient, ModbusPDU], None]):
        self._on_receive_pdu = callback

    def set_on_connection_change_callback(self, callback: Callable[[ModbusDeviceClient, bool, str, int], None]):
        self._on_connection_change_callback = callback

    def _trigger_on_send_pdu(self, device_client: ModbusDeviceClient, pdu: ModbusPDU):
        if callable(self._on_send_pdu):
            try:
                self._on_send_pdu(device_client, pdu)
            except Exception as e:
                self.logger.error(f"Exception in on send pdu callback {e}")

    def _trigger_on_receive_pdu(self, device_client: ModbusDeviceClient, pdu: ModbusPDU):
        if callable(self._on_receive_pdu):
            try:
                self._on_receive_pdu(device_client, pdu)
            except Exception as e:
                self.logger.error(f"Exception in on receive pdu callback {e}")

    def _trigger_on_connection_change(self, device_client: ModbusDeviceClient, connected: bool):
        if connected:
            self.logger.info(f"Connected to {device_client.server_id} ({device_client.server_address}:{device_client.server_port})")
        else:
            self.logger.info(f"Disconnected from {device_client.server_id} ({device_client.server_address}:{device_client.server_port})")

        if callable(self._on_connection_change_callback):
            try:
                self._on_connection_change_callback(device_client, connected, device_client.server_address, device_client.server_port)
            except Exception as e:
                self.logger.error(f"Exception in on_connection_change callback {e}")

    def get_value_definition(self, data_point_identifier: str) -> Optional[ModbusClientValueDefinition]:
        return self._value_definition_by_data_point.get(data_point_identifier)

    def set_data_points(self, data_point_list: list):
        """
        Adds the given data points to this client.
        For each data point, the respective server device and unit are registered and a ModbusValueDefinition is created.
        Args:
            data_point_list: A list of data point dict representations to register with this client.
        """
        for data_point in data_point_list:
            if data_point.get("protocol") != ProtocolName.MODBUS_TCP.value:
                self.logger.warning(f"DataPoint with protocol {data_point.get('protocol')} is not supported by ModbusClient")
                continue
            self._data_points[data_point["identifier"]] = data_point
            # Create ModbusDeviceClient if required
            server_id = data_point.get("protocol_server_id")
            protocol_data = data_point.get("protocol_data", {})
            unit_id = protocol_data.get("unit_id")

            server_address = self._server_data.get(server_id, {}).get("ip")
            server_port = self._server_data.get(server_id, {}).get("port", 502)
            if server_address is None:
                raise ValueError(f"No IP address given for server {server_id}")

            if server_id not in self._servers:
                modbus_device_client = ModbusDeviceClient(server_id=server_id, server_address=server_address, server_port=server_port)
                modbus_device_client.modbus_client = self
                self._servers[server_id] = modbus_device_client
            else:
                modbus_device_client = self._servers[server_id]

            # Create ModbusUnitClient if required
            if unit_id is None:
                raise ValueError(f"No unit id given for data point {data_point['identifier']}")
            if not modbus_device_client.has_unit(unit_id):
                server_expects_zero_based_addressing = True
                modbus_unit_client = ModbusUnitClient(unit_id=unit_id,
                                                      zero_based_addressing=server_expects_zero_based_addressing,
                                                      device_client=modbus_device_client)
                modbus_device_client.add_unit(modbus_unit_client)
                self.logger.info(f"Registering Modbus Unit {unit_id} at server {server_id}")
            else:
                modbus_unit_client = modbus_device_client.get_unit(unit_id)

            # Create ValueDefinition
            table = ModbusTable(protocol_data["table"])
            value_definition = ModbusClientValueDefinition(
                data_point_identifier=data_point["identifier"],
                unit_id=unit_id,
                modbus_table=table,
                type_id=protocol_data["type_id"],
                register_width=protocol_data["width"],
                start_address=protocol_data["address"],
                endian=ModbusEndian(protocol_data["endian"]),
                periodicity=protocol_data.get("polling_interval", 0) if protocol_data.get("polling_enabled", False) else None,
                address_is_zero_based=protocol_data.get("zero_based_address", True),
                unit_client=modbus_unit_client
            )
            self._value_definition_by_data_point[data_point["identifier"]] = value_definition
            modbus_unit_client.register_value_definition(value_definition)
            ## Callbacks
            value_definition.on_value_update_callback = self._on_value_update
            ## Ensure table offset is valid
            table_offset = protocol_data.get("table_offset", 0)
            modbus_unit_client.register_table_offset(table, table_offset, True)

    def start(self, do_general_interrogation: bool = True, enable_polling: bool = True):
        """
        Initializes connections to all registered devices.
        Args:
            do_general_interrogation: Whether to perform a general interrogation for each device (querying all known values).
            enable_polling: Whether to enable polling for values with configured periodicity.
        """
        super(ModbusClient, self).start()
        # Create Clients
        for server_id, device_client in self._servers.items():
            self.logger.info(f"Creating Client for {server_id} ({device_client.server_address}:{device_client.server_port})")
            self._connected_events[server_id] = threading.Event()
            thread = threading.Thread(target=self._client_thread, args=(server_id, do_general_interrogation))
            thread.start()
            self._client_threads[server_id] = thread
            if enable_polling:
                periodic_thread = threading.Thread(target=self._periodic_thread, args=(server_id, ))
                periodic_thread.start()
                self._client_periodic_threads[server_id] = periodic_thread
        if do_general_interrogation:
            self.general_interrogation()

    def general_interrogation(self, device_id: Optional[str] = None, unit_id: Optional[int] = None) -> bool:
        """
        Performs a general interrogation for the specified device ID and specified unit ID.
        If no device is specified, all known devices are selected.
        If no unit is specified, all known units for each selected device are interrogated.
        Args:
            device_id: The ID of the device to interrogate (or None to interrogate all known devices).
            unit_id: The ID of the unit to interrogate (or None to interrogate all known units).
        """
        values_to_query = []

        if device_id is None:
            devices = list(self._servers.keys())
        else:
            devices = [device_id]
        for server_id in devices:
            device_client = self._servers.get(server_id)
            if device_client is None:
                raise ValueError(f"No device registered for server {server_id}")
            if unit_id is None:
                unit_ids = list(device_client.units.keys())
            else:
                unit_ids = [unit_id]
            for known_unit_id in unit_ids:
                if not device_client.has_unit(known_unit_id):
                    raise ValueError(f"Device {server_id} has no unit with ID {known_unit_id} - only got {unit_ids}")
                unit = device_client.get_unit(known_unit_id)
                values_to_query.extend(unit.get_value_definition_list())
        success = True
        if not self.wait_until_connected(10):
            self.logger.error("Failed to connect to server")
            return False
        for value_definition in values_to_query:
            success &= value_definition.async_read(global_notify_read_done=False)
        return success

    def get_client_loop(self, device_id: str) -> asyncio.AbstractEventLoop:
        return self._client_loops[device_id]

    def get_device_client(self, device_id: str) -> Optional[ModbusDeviceClient]:
        return self._servers.get(device_id)

    def run(self):
        set_thread_name("W/MB/C/Main")
        while not self._stop_requested.is_set():
            self._stop_requested.wait(10)

    def stop(self, timeout: Optional[float] = None):
        self._stop_requested.set()
        for loop, event in self._periodic_loops:
            loop.call_soon_threadsafe(event.set)
        for thread in self._client_periodic_threads.values():
            if thread.is_alive():
                thread.join(timeout=timeout)

        for server_id, thread in self._client_threads.items():
            client = self._pymodbus_clients.get(server_id)
            client_loop = self.get_client_loop(server_id)
            if client is not None:
                try:
                    self.logger.info(f"Stopping Client for {server_id}")
                    client_loop.call_soon_threadsafe(client.close)
                except Exception as e:
                    self.logger.error(f"Failed to close Client for {server_id}: {e}")
            if thread.is_alive():
                try:
                    thread.join(timeout=timeout)
                except RuntimeError:
                    pass
                if thread.is_alive():
                    self.logger.error(f"Failed to join thread of server {server_id}")

    def wait_until_connected(self, timeout: Optional[float] = None) -> bool:
        """
        Waits until the client is connected to all servers.
        Args:
            timeout: An optional timeout in seconds to wait.

        Returns:
            True iff the client is connected to all servers.

        Raises:
            RuntimeError: If the client thread is not alive.
        """
        if not self.is_alive():
            raise RuntimeError(f"Cannot wait for connections while client is not running")
        wait_time_start = time.time()
        remaining_timeout = timeout

        for server_id, connected_event in self._connected_events.items():
            if not connected_event.wait(remaining_timeout):
                self.logger.debug(f"Not connected to server {server_id}")
                return False
            if remaining_timeout is not None:
                remaining_timeout = remaining_timeout - (time.time() - wait_time_start)
                if remaining_timeout <= 0:
                    return False

        return True

    def _client_thread(self, server_id: str, do_general_interrogation: bool):
        """
        To be run in a distinct thread for handling a single server connection.
        """
        device_client = self._servers[server_id]

        set_thread_name(f"W/MB/C/{server_id}")

        async def run_client():
            def _trace_pdu(sent, pdu):
                if sent:
                    self._trigger_on_send_pdu(device_client, pdu)
                else:
                    self._trigger_on_receive_pdu(device_client, pdu)
                return pdu

            def _trace_connect(connected: bool):
                self._trigger_on_connection_change(device_client, connected)

            client = AsyncPyModbusTcpClient(
                device_client.server_address,
                port=device_client.server_port,
                reconnect_delay=1,
                reconnect_delay_max=10,
                trace_pdu=_trace_pdu,
                trace_connect=_trace_connect
            )
            self._pymodbus_clients[server_id] = client
            # Initialize connection status
            self._trigger_on_connection_change(device_client, False)
            while not self._stop_requested.is_set():
                try:
                    self.logger.info(f"Connecting to {server_id} ({device_client.server_address}:{device_client.server_port})")
                    await client.connect()
                    if client.connected:
                        break
                    self.logger.warning(f"Could not connect to server {server_id} ({device_client.server_address}:{device_client.server_port})")
                    await asyncio.sleep(1)
                except Exception as e:
                    self.logger.error(f"Failed to connect to server {server_id}: {e}")
                    continue
            self._connected_events[server_id].set()
            self.logger.info(f"Connected to server {server_id} ({device_client.server_address}:{device_client.server_port})")

        loop = asyncio.new_event_loop()
        loop.set_debug(True)
        self._client_loops[server_id] = loop
        asyncio.set_event_loop(loop)
        loop.run_until_complete(run_client())
        loop.run_forever()
        # asyncio.run(run_client(), debug=True)

    ###
    ### PERIODIC POLLING
    ###
    def _periodic_thread(self, server_id: str):
        """
        Runs the periodic tasks for the given server.
        """
        set_thread_name(f"W/MB/C/P/{server_id}")
        loop = asyncio.new_event_loop()
        event = asyncio.Event()
        self._periodic_loops.append((loop, event))
        periodic_tasks = {}
        device_client = self._servers[server_id]
        for unit_id, unit_client in device_client.units.items():
            for value_definition in unit_client.get_value_definition_list():
                if value_definition.periodicity is not None:
                    logger = self.logger.getChild(f"Periodic-{value_definition.data_point_identifier}")
                    periodic_task = PeriodicTask(period_seconds=value_definition.periodicity,
                                                 callback=self._periodic_read,
                                                 callback_args=[value_definition],
                                                 logger=logger)
                    periodic_tasks[value_definition.data_point_identifier] = periodic_task

        async def start_periodic_tasks():
            for _periodic_task in periodic_tasks.values():
                await _periodic_task.start()
            await event.wait()
            for _periodic_task in periodic_tasks.values():
                await _periodic_task.stop()
        loop.run_until_complete(start_periodic_tasks())

    def _periodic_read(self, value_definition: ModbusClientValueDefinition):
        self.logger.debug(f"Periodic reading for {value_definition.data_point_identifier}")
        value_definition.read_raw(global_notify_read_done=False)

    ###
    ### READ DATA POINTS AND REGISTERS
    ###
    def _read_from_table_future(
            self, server_id: str, modbus_table: ModbusTable, address: int, count: int, unit_id: int, trim: bool = True) -> Future:
        loop = self.get_client_loop(server_id)
        client = self._pymodbus_clients.get(server_id)
        relative_address = address
        match modbus_table:
            case ModbusTable.COIL:
                a_data = client.read_coils(address=relative_address, count=count, device_id=unit_id)
            case ModbusTable.DISCRETE_INPUT:
                a_data = client.read_discrete_inputs(address=relative_address, count=count, device_id=unit_id)
            case ModbusTable.INPUT_REGISTER:
                a_data = client.read_input_registers(address=relative_address, count=count, device_id=unit_id)
            case ModbusTable.HOLDING_REGISTER:
                a_data = client.read_holding_registers(address=relative_address, count=count, device_id=unit_id)
            case _:
                raise ModbusError(f"Cannot read value - invalid table {modbus_table.name}")
        return asyncio.run_coroutine_threadsafe(a_data, loop)

    def read_from_table(self, server_id: str, modbus_table: ModbusTable, address: int, count: int,
                        unit_id: int, trim: bool = True, error_as_none: bool = True) -> List[int] | List[bool] | None:
        """
        Reads the specified address range from the server.
        Args:
            server_id: The server to query
            modbus_table: The modbus table to query
            address: The first (relative) address to read
            count: The number of registers / coils to read
            unit_id: The ID of the Modbus unit to query
            trim: Whether to trim the resulting value list to the specified number of register values (count)
            error_as_none: Whether to catch exceptions and return None if the value cannot be read

        Returns:
            The values of the queries registers (int) or coils (bool).

        Raises:
            ModbusError: If an internal error occurs.
            ModbusResponseError: If the server indicates an error.
        """
        try:
            future = self._read_from_table_future(server_id, modbus_table, address, count, unit_id, trim)
        except Exception as e:
            if error_as_none:
                self.logger.error(f"Error reading from {modbus_table.name} {address}: {e}")
                return None
            raise e

        try:
            pdu = future.result()
        except Exception as e:
            if error_as_none:
                return None
            raise ModbusError(f"Failed to read {modbus_table.name}: {e}")

        if pdu.isError():
            if error_as_none:
                return None
            raise ModbusResponseError(ExcCodes(pdu.exception_code))
        # self.logger.info(pdu)
        if modbus_table.is_register():
            data = pdu.registers
        else:
            data = pdu.bits
            if trim:
                if len(data) > count:
                    data = data[:count]
        return data

    def read_raw_data_point(self, data_point_identifier: str, error_as_none: bool = True,
                            global_notify_read_done: bool = True) -> List[int] | List[bool] | None:
        """
        Reads the given data point from the respective server (blocking).
        This uses Function Codes 1, 2, 3 or 4.
        Args:
            data_point_identifier: The data point identifier to read.
            error_as_none: Whether to catch exceptions and return None if the value cannot be read
            global_notify_read_done: Whether to call the global read callback.

        Returns:
            The value read from the respective server as raw register values.

        Raises:
            ModbusError: If an error occurs during reading.
            ModbusResponseError: If the server indicates an error.
        """
        event: threading.Event = threading.Event()
        success = False
        error_code = None

        def callback(_value_def: ModbusValueDefinition, _success: bool, _error_code: Optional[int], _values: List[int] | List[bool], _value: ModbusValueType):
            nonlocal success, event, error_code
            success = _success
            error_code = _error_code
            event.set()

        if not self.read_data_point_callback(data_point_identifier, callback, error_as_false=error_as_none, global_notify_read_done=global_notify_read_done):
            return None

        event.wait()
        value_definition = self._value_definition_by_data_point.get(data_point_identifier)
        if success:
            return value_definition.get_raw_value()
        if error_as_none:
            return None
        raise ModbusError(f"Could not read data point {data_point_identifier}: {error_code=}")

    def read_data_point(self, data_point_identifier: str, error_as_none: bool = False,
                        global_notify_read_done: bool = True) -> ModbusValueType:
        """
        Reads the given data point from the respective server (blocking).
        This uses Function Codes 1, 2, 3 or 4.
        Args:
            data_point_identifier: The data point identifier to read.
            error_as_none: Whether to catch exceptions and return None if the value cannot be read
            global_notify_read_done: Whether to call the global read callback.

        Returns:
            The value read from the respective server.

        Raises:
            ModbusError: If an error occurs during reading.
            ModbusResponseError: If the server indicates an error.
        """
        data = self.read_raw_data_point(data_point_identifier, error_as_none=error_as_none, global_notify_read_done=global_notify_read_done)
        value_definition = self._value_definition_by_data_point.get(data_point_identifier)
        value_definition.set_raw_value(data)
        return value_definition.get_value()

    def read_data_point_callback(
            self,
            data_point_identifier: str,
            callback: Optional[Callable[[ModbusValueDefinition, bool, Optional[int], List[int] | List[bool], Optional[ModbusValueType]], None]] = None,
            error_as_false: bool = True,
            global_notify_read_done: bool = True,
    ) -> bool:
        """
        Asynchronously reads the value of the specified data point from the respective server.
        This uses Function Codes 1, 2, 3 or 4
        Args:
            data_point_identifier: The identifier of the data point to read.
            callback: An optional callback to call once the data point is read.
            error_as_false: Whether to catch exceptions and return False in this case.
            global_notify_read_done: Whether to call the global on_read_callback.
        """
        value_definition = self._value_definition_by_data_point.get(data_point_identifier)
        if value_definition is None:
            if error_as_false:
                return False
            raise ModbusError(f"Unknown data point {data_point_identifier}")
        server_id = value_definition.unit_client.device_client.server_id
        loop = self._client_loops.get(server_id)
        client = self._pymodbus_clients.get(server_id)
        if client is None or loop is None:
            if error_as_false:
                return False
            raise ModbusError(f"No client for data point {data_point_identifier}")
        relative_address = value_definition.get_relative_address()

        self.logger.debug(f"Reading {value_definition.modbus_table.name} at {relative_address} (length {value_definition.register_width}) [CB]")
        try:
            future = self._read_from_table_future(
                server_id=server_id, modbus_table=value_definition.modbus_table,
                address=relative_address, count=value_definition.register_width, unit_id=value_definition.unit_id
            )
        except Exception as e:
            self._resolve_data_point_read_future(value_definition, None, callback, trigger_global_callback=global_notify_read_done)
            if error_as_false:
                return False
            raise e

        def _future_done(_future):
            self._resolve_data_point_read_future(value_definition, _future, callback, trigger_global_callback=global_notify_read_done)

        future.add_done_callback(_future_done)
        return True

    def _resolve_data_point_read_future(self,
                                        value_definition: ModbusClientValueDefinition,
                                        future: Optional[Future],
                                        callback: Optional[Callable],
                                        trigger_global_callback: bool = True):
        success = True
        data = []
        value = None
        server_exception = None
        if future is not None:
            try:
                success = not future.cancelled()
                pdu: ModbusPDU = future.result()
                if pdu.isError():
                    success = False
                    server_exception = pdu.exception_code
                else:
                    if value_definition.modbus_table.is_register():
                        data = pdu.registers
                    else:
                        data = pdu.bits
                    if len(data) > value_definition.register_width:
                        data = data[:value_definition.register_width]
                    value_definition.set_value(value_definition.decode_value(data))
                    value = value_definition.get_value()
            except Exception as e:
                success = False
                server_exception = -1

        if trigger_global_callback and callable(self._on_read_callback):
            try:
                self._on_read_callback(value_definition, success, server_exception, data, value)
            except Exception as e:
                self.logger.error(f"Exception in on_read_callback: {e}")

        if callable(callback):
            try:
                callback(value_definition, success, server_exception, data, value)
            except Exception as e:
                self.logger.error(f"Exception in resolve_data_point_read callback ({e=})")

    ###
    ### WRITE DATA POINTS AND REGISTERS
    ###
    def _write_to_table_future(self,
                               server_id: str, modbus_table: ModbusTable, address: int, count: int, unit_id: int, values: List[int] | List[bool]) -> Future:
        loop = self.get_client_loop(server_id)
        client = self._pymodbus_clients.get(server_id)
        relative_address = address
        if len(values) != count:
            raise ValueError(f"Expected {count} values, got {len(values)}")

        match modbus_table:
            case ModbusTable.COIL:
                if count == 1:
                    a_data = client.write_coil(address=relative_address, value=bool(values[0]), device_id=unit_id)
                else:
                    a_data = client.write_coils(address=relative_address, values=values, device_id=unit_id)
            case ModbusTable.DISCRETE_INPUT:
                raise ModbusError("Cannot write to discrete input")
            case ModbusTable.INPUT_REGISTER:
                raise ModbusError("Cannot write to input register")
            case ModbusTable.HOLDING_REGISTER:
                if len(values) == 1:
                    a_data = client.write_register(address=relative_address, value=values[0], device_id=unit_id)
                else:
                    a_data = client.write_registers(address=relative_address, values=values, device_id=unit_id)
            case _:
                raise ModbusError(f"Cannot write value - invalid table {modbus_table.name}")
        return asyncio.run_coroutine_threadsafe(a_data, loop)

    def write_raw_data_point(self, data_point_identifier: str, values: List[int] | List[bool]) -> bool:
        """
        Writes a raw value to the specified data point register(s) at the server (blocking)
        Args:
            data_point_identifier: The identifier of the data point to write.
            values: The raw register values to write.

        Returns:
            True iff the write was successful.

        Raises:
            ModbusError: If there is an error writing the value.
            KeyError: If the data point is invalid.
            ValueError: If the value is invalid.
        """
        success: bool = False
        callback_done: threading.Event = threading.Event()

        def callback(_value_definition: ModbusValueDefinition, _success: bool, _error_code: Optional[int], _values: List[int] | List[bool], _value: ModbusValueType):
            nonlocal success
            success = _success
            callback_done.set()

        try:
            self.write_raw_data_point_callback(data_point_identifier, values, callback)
        except Exception as e:
            self.logger.error(f"Failed to write values {values} for {data_point_identifier}: {e}")
            return False
        callback_done.wait()
        return success

    def write_data_point(self, data_point_identifier: str, value: ModbusValueType) -> bool:
        """
        Writes the given value to the server for the respective data point (blocking)
        Args:
            data_point_identifier: The identifier of the data point to write.
            value: The value to write.

        Returns:
            True if the write was successful.

        Raises:
            ModbusError: If an error occurs while writing the data point.
            KeyError: If the data point is invalid.
            ValueError: If the value is invalid.
        """
        success: bool = False
        callback_done: threading.Event = threading.Event()

        def callback(_value_definition, _success, _error_code, _values, _value):
            nonlocal success
            success = _success
            callback_done.set()

        try:
            self.write_data_point_callback(data_point_identifier, value, callback)
        except Exception as e:
            self.logger.error(f"Failed to write value {value} to {data_point_identifier}: {e}")
            return False
        callback_done.wait()
        return success

    def write_raw_data_point_callback(
            self,
            data_point_identifier: str,
            values: List[bool] | List[int],
            callback: Optional[Callable[[ModbusValueDefinition, bool, Optional[int], List[int] | List[bool], ModbusValueType], None]] = None,
            error_as_false: bool = True
    ):
        """
        Asynchronously writes the raw given value to the specified data point register(s) at the respective server.
        Args:
            data_point_identifier: The identifier of the data point to write.
            values: The raw values to write to the server
            callback: An optional callback to call once the data point is written. It is called with the value definition, a success indicator,
                the raw register values and the native value type.
            error_as_false: If True, exceptions will be caught as False is returned.
        """
        value_definition = self._value_definition_by_data_point.get(data_point_identifier)
        if value_definition is None:
            if error_as_false:
                return False
            raise KeyError(f"Unknown data point {data_point_identifier}")
        if len(values) != value_definition.register_width:
            if error_as_false:
                return False
            raise ValueError(f"Expected {value_definition.register_width} values, got {len(values)}")
        server_id = value_definition.unit_client.device_client.server_id
        loop = self._client_loops.get(server_id)
        client = self._pymodbus_clients.get(server_id)
        if client is None or loop is None:
            if error_as_false:
                return False
            raise ModbusError(f"No client for data point {data_point_identifier}")
        relative_address = value_definition.get_relative_address()
        try:
            future = self._write_to_table_future(server_id=server_id, modbus_table=value_definition.modbus_table, address=relative_address,
                                                 count=value_definition.register_width, unit_id=value_definition.unit_id, values=values)
        except Exception as e:
            self._resolve_data_point_write_future(
                value_definition, future=None, values=values, value=value_definition.decode_value(values), callback=callback)
            if error_as_false:
                return False
            raise e

        def _future_done(_future):
            self._resolve_data_point_write_future(value_definition, _future, values, value_definition.decode_value(values), callback)

        future.add_done_callback(_future_done)
        return True

    def write_data_point_callback(
            self,
            data_point_identifier: str,
            value: ModbusValueType,
            callback: Optional[Callable[[ModbusValueDefinition, bool, Optional[int], List[int] | List[bool], ModbusValueType], None]] = None,
            error_as_false: bool = True
            ):
        """
        Asynchronously writes the value of the specified data point to the respective server.
        Args:
            data_point_identifier: The identifier of the data point to write.
            value: The value to write to the server
            callback: An optional callback to call once the data point is written. It is called with the value definition, a success indicator,
                the raw register values and the native value type.
            error_as_false: If True, exceptions will be caught as False is returned.
        """
        value_definition = self._value_definition_by_data_point.get(data_point_identifier)
        if value_definition is None:
            if error_as_false:
                return False
            raise KeyError(f"Unknown data point {data_point_identifier}")
        raw_value = value_definition.encode_value(value)
        return self.write_raw_data_point_callback(data_point_identifier, raw_value, callback, error_as_false=error_as_false)

    def _resolve_data_point_write_future(
            self,
            value_definition: ModbusClientValueDefinition, future: Optional[Future], values: List[int] | List[bool],
            value: ModbusValueType, callback: Optional[Callable]):
        success = True
        exception_code = None
        if future is not None:
            try:
                success = not future.cancelled()
                pdu: ModbusPDU = future.result()
                if pdu.isError():
                    success = False
                    exception_code = pdu.exception_code

                if success:
                    # TODO: Use silent_set_value?
                    value_definition.set_value(value)
            except Exception as e:
                success = False
                exception_code = -1

        if callable(self._on_write_callback):
            try:
                self._on_write_callback(value_definition, success, exception_code, values, value)
            except Exception as e:
                self.logger.error(f"Exception in on_write_callback, {e=}")

        if callable(callback):
            try:
                callback(value_definition, success, exception_code, values, value)
            except Exception as e:
                self.logger.error(f"Exception in resolve_data_point_write callback, {e=}")

    ###
    ### CALLBACK WRAPPERS
    ###
    def _on_value_update(self, value_definition: ModbusClientValueDefinition, old_value: Optional[ModbusValueType], new_value: Optional[ModbusValueType]):
        if callable(self._on_value_update_callback):
            try:
                self._on_value_update_callback(value_definition, old_value, new_value)
            except Exception as e:
                self.logger.warning(f"Error in on_value_update_callback for {value_definition.data_point_identifier}: {e}")
