import copy
import threading
import threading as th
import traceback
from queue import Empty, Queue
from typing import Union, TYPE_CHECKING, Optional, List

import c104
import pyprctl

from wattson.iec104.common import ConnectionState, GLOBAL_COA
from wattson.iec104.common.config import *
from wattson.iec104.implementations.c104 import C104Point, build_apdu_from_c104_bytes, explain_bytes
from wattson.iec104.interface.apdus import APDU, I_FORMAT
from wattson.iec104.interface.client import IECClientInterface as IECClientInterface
from wattson.iec104.interface.types import TypeID, COT

if TYPE_CHECKING:
    pass


class FakeLock:
    def __enter__(self):
        return

    def __exit__(self, exc_type, exc_val, exc_tb):
        return


class IEC104Client(IECClientInterface, th.Thread):
    """ Currently having to hack local ConnectionState as Interro-Status is not available :/"""
    def __init__(self, org: int = 0, **kwargs):
        """
        Initializes the client and sets up internal callbacks as requested.
        :param org: COA for this client. Defaults to 0.
        :return: None
        """
        #c104.set_debug_mode(0)
        #c104.set_debug_mode(c104.Debug.Message | c104.Debug.Point | c104.Debug.Gil)
        #c104.set_debug_mode(c104.Debug.Point | c104.Debug.Client | c104.Debug.Connection | c104.Debug.Callback | c104.Debug.Station | c104.Debug.Gil)
                            # | c104.Debug.Callback | c104.Debug.Connection | c104.Debug.Gil)

        th.Thread.__init__(self)
        IECClientInterface.__init__(self, **kwargs)
        #self.logger.setLevel(logging.DEBUG)
        #self.logger.setLevel(logging.INFO)

        self.do_general_interrogation = kwargs.get("do_general_interrogation", True)
        self.do_clock_sync = kwargs.get("do_clock_sync", True)
        self.add_monitoring_points = False
        self._connect_delay_s = kwargs.get("connect_delay_s", 0)
        self._reconnect_interval_s = kwargs.get("reconnect_interval_s", 10)

        self.logger.info(c104.Init)

        self.name = "c104client"

        #self._cb_lock = threading.RLock()
        self._cb_lock = FakeLock()

        self.c104_init = c104.Init.NONE
        if self.do_general_interrogation and self.do_clock_sync:
            self.c104_init = c104.Init.ALL
        elif self.do_general_interrogation:
            self.c104_init = c104.Init.INTERROGATION
        elif self.do_clock_sync:
            self.c104_init = c104.Init.CLOCK_SYNC

        self._org = org
        self._servers = {}
        self._client = c104.Client(
            tick_rate_ms=CLIENT_TICKRATE_MS,
            command_timeout_ms=CLIENT_COMMAND_TIMEOUT_MS
        )
        self._client.on_new_point(callable=self._on_new_point)
        self._client.on_new_station(callable=self._on_new_station)

        self._send_queue: Queue = Queue()
        self._receive_queue: Queue = Queue()
        # TODO: Locks for Connection status

        self.on_receive_datapoint_callback = kwargs.get('on_receive_datapoint')

        self._terminate = threading.Event()
        self._force_shutdown = False

    def get_receive_queue(self) -> Queue:
        """
        Returns the queue.Queue used for received messages.
        :return: The receive queue.Queue
        """
        return self._receive_queue

    def start(self):
        """
        Starts the client and all managed connections.
        :return:
        """
        self.logger.debug("Start Super")
        self.logger.debug("Bind")
        self.bind()
        self.logger.debug("Start Client")
        self._client.start()
        self.logger.debug("Connect to Servers")
        th.Thread.start(self)

    def stop(self, force: bool = False):
        """
        Requests an asynchronous stop for this client.
        For waiting for a complete shutdown, use join on this client afterwards.
        :param force: Force the stop regardless of client state.
        :return:
        """
        self.logger.info("Stopping")
        with self._cb_lock:
            self._terminate.set()
            self._force_shutdown = force

    def send(self, coa: int, ioa: int, cot: c104.Cot):
        """
        Queues the data point identified by COA and IOA for transmission.
        The given COT will be used during this transmission.

        :param coa: The Common Address of the respective Server
        :param ioa: The Data Point's IOA
        :param cot: The COT to use for transmission
        :return:
        """
        if not self.has_datapoint(coa, ioa):
            self.logger.warning(f"Cannot send non-existent Datapoint {coa}.{ioa}")
            return False
        self.logger.info(f"Queuing Sending of Datapoint {coa}.{ioa} -- COT = {cot}")
        self._send_queue.put((coa, ioa, cot))
        return True

    def has_server(self, coa: int) -> bool:
        """
        Checks whether the requested server COA exists.
        :param coa: The COA to check for
        :return: True iff a server with the given COA exists in this client.
        """
        return str(coa) in self._servers

    def get_servers(self) -> List:
        """
        Returns a list of COAs of RTUs / servers that are known to this client.
        :return: A list of COAs
        """
        return copy.deepcopy(list(self._servers.keys()))

    def add_server(self, ip: str, coa: int, **kwargs) -> Union[bool, int]:
        """
        Adds a new RTU's server to the cache.

        :param ip: IP the server is publishing from
        :param coa: RTUs COA

        :return: False if coa already known to client, otherwise coa of newly inserted server
        """
        if str(coa) in self._servers:
            return False

        args = {
            "port": SERVER_DEFAULT_PORT
        }
        args.update(kwargs)
        self._servers[str(coa)] = {
            "ip": ip,
            "port": args["port"],
            "connected": False,
            "attempted_connect": False,
            "coa": coa,
            "state": ConnectionState.UNATTEMPTED_TO_CONNECT,
            "connection": None,
            "station": None,
            "connection_state": c104.ConnectionState.CLOSED,
            "last_connection_state": c104.ConnectionState.CLOSED,
            "datapoints": {}
        }
        return coa

    def get_server_IP(self, coa: int) -> str:
        """
        Returns the IP of the RTU / server identified by the given COA.
        :param coa: The COA of the RTU
        :raises: ValueError if the COA is not known.
        :return: The IP address of the server / RTU.
        """
        if not self.has_server(coa):
            raise ValueError("No server for that coa available.")
        return self._get_server(coa)["ip"]

    def get_server_port(self, coa: int) -> int:
        """
        Returns the port used by the RTU server identified by the given COA.
        :param coa: The COA of the RTU / Server to get the port of
        :return: The port number of the requested RTU or -1 if the RTU is unknown.
        """
        with self._cb_lock:
            if not self.has_server(coa):
                return -1
            return self._get_server(coa)['port']

    def get_connection_state(self, coa) -> c104.ConnectionState:
        with self._cb_lock:
            return self._get_server(coa)["connection_state"]

    def get_wattson_connection_state(self, coa: int) -> ConnectionState:
        """
        Returns the connection state of the RTU identified by the given COA.

        :param coa: ID of RTU
        :raise ValueError: For unknown COA
        :return: The ConnectionState
        """
        state_map = {
            c104.ConnectionState.CLOSED: ConnectionState.CLOSED,
            c104.ConnectionState.CLOSED_AWAIT_OPEN: ConnectionState.CLOSED,
            c104.ConnectionState.CLOSED_AWAIT_RECONNECT: ConnectionState.CLOSED,
            c104.ConnectionState.OPEN: ConnectionState.INTERRO_DONE,
            c104.ConnectionState.OPEN_AWAIT_CLOCK_SYNC: ConnectionState.OPEN,
            c104.ConnectionState.OPEN_AWAIT_CLOSED: ConnectionState.OPEN,
            c104.ConnectionState.OPEN_AWAIT_INTERROGATION: ConnectionState.INTERRO_STARTED,
            c104.ConnectionState.OPEN_MUTED: ConnectionState.OPEN,
        }
        connection_state = self.get_connection_state(coa)
        return state_map.get(connection_state, ConnectionState.CLOSED)

    def connect_server(self, server: Union[str, int, dict]):
        """
        (Re-)Establishes a connection to the given server / RTU.
        :param server: The COA of the server as str or int, or the server dictionary
        :return: None
        """
        server = self._get_server(server)
        if server["connection"] is None:
            self.logger.info(f"Adding Connection {server['ip']} // {server['coa']}")
            try:
                server["connection"] = self._client.add_connection(
                    ip=server["ip"],
                    port=server["port"],
                    init=self.c104_init
                )
            except RuntimeError as e:
                self.logger.error(f"Failed to add connection: {e=}")

            self.logger.debug(f"Setting Connection Callbacks for {server['coa']}")
            server["connection"].on_receive_raw(callable=self._on_receive_raw_callback_wrapper)
            server["connection"].on_send_raw(callable=self._on_send_raw_callback_wrapper)
            server["connection"].on_state_change(callable=self._on_connection_state_change)

            self.logger.debug(f"Adding Station for {server['coa']}")
            server["station"] = server["connection"].add_station(
                common_address=server["coa"]
            )

            # control direction datapoints are not
            # transmitted during General Interrogation
            for data_point in self.datapoints:
                if data_point["protocol"] != "60870-5-104":
                    continue
                dp = data_point["protocol_data"]
                if dp["coa"] != server["coa"]:
                    continue
                if dp["type_id"] in range(45, 70):
                    # only specifically add set-datapoints, (most) of the others shoudl be auto-added
                    # upon the general-interrogation
                    point = server["station"].add_point(io_address=int(dp["ioa"]), type=c104.Type(dp["type_id"]))
                    if point is None:
                        self.logger.error(f"Could not add DP {dp['coa']}.{dp['ioa']} with type {dp['type_id']}")
                    else:
                        point.on_receive(callable=self._on_receive_datapoint)
                        server["datapoints"][str(dp["ioa"])] = C104Point(point)
                if dp["type_id"] < 45 and self.add_monitoring_points:
                    self.logger.info("Adding monitoring points")
                    point = server["station"].add_point(io_address=int(dp["ioa"]), type=c104.Type(dp["type_id"]))
                    if point is None:
                        self.logger.error(f"Could not add DP {dp['coa']}.{dp['ioa']} with type {dp['type_id']}")
                    else:
                        self.logger.error(f"Added DP {dp['coa']}.{dp['ioa']} with type {dp['type_id']}")
                        point.on_receive(callable=self._on_receive_datapoint)
                        server["datapoints"][str(dp["ioa"])] = C104Point(point)

            self.logger.debug(f"Storing Server {server['coa']}")
            self._servers[str(server["coa"])] = server

        if not server["connection"].is_connected:
            self.logger.info(f"Connecting to COA {server['coa']}")
            success = server["connection"].connect()
            new_state = ConnectionState.OPEN if success else ConnectionState.CLOSED
            server['state'] = new_state
            return success

        return True

    def _on_connection_state_change(self, connection: c104.Connection, state: c104.ConnectionState) -> None:
        ip = connection.ip
        server = self.find_server_by_ip(ip)
        if server is None:
            self.logger.error(f"Cannot find server for connection to {ip=}")
            return
        coa = server["coa"]
        with self._cb_lock:
            server["last_connection_state"] = server.get("connection_state", c104.ConnectionState.CLOSED)
            server["connection_state"] = state
            server["connected"] = state == c104.ConnectionState.OPEN
            last_state = server["last_connection_state"]

        if last_state != state:
            self.logger.info(f"RTU {coa}: {state.name} (Connected: {connection.is_connected})")
            self._on_connection_change_wrapper(coa, server)
        server["connection_event"].set()

    def _on_connection_change_wrapper(self, coa: int, server):
        if self.callbacks.get("on_connection_change") is not None:
            try:
                with self._cb_lock:
                    self.callbacks["on_connection_change"](coa, self._is_server_connected(server), server["ip"], server["port"])
            except Exception as e:
                self.logger.critical(f"_on_connection_change Error: {e=}")

    def _on_receive_raw_callback_wrapper(self, connection: c104.Connection, data: bytes) -> None:
        with self._cb_lock:
            try:
                station: c104.Station = connection.stations[0]
                coa = station.common_address
                apdu = build_apdu_from_c104_bytes(data)
                if isinstance(apdu, I_FORMAT) and 22 in apdu.ioas:
                    s = f"Pot. bad apdu {apdu} build from\n{data}\n{explain_bytes(data)}"
                    self.logger.critical(s)
                self._update_conn_state_if_interro_APDU(apdu, coa)
                self.callbacks['on_receive_apdu'](apdu, coa, True)
            except Exception as e:
                self.logger.critical(f'On-rcv raw error {e=}')

    def _on_send_raw_callback_wrapper(self, connection: c104.Connection, data: bytes) -> None:
        with self._cb_lock:
            try:
                station: c104.Station = connection.stations[0]
                coa = station.common_address
                apdu = build_apdu_from_c104_bytes(data)
                self._update_conn_state_if_interro_APDU(apdu, coa)
                self.callbacks['on_send_apdu'](apdu, coa)
            except Exception as e:
                self.logger.error(f"On-send raw error {e=}")

    def server_watchdog(self, server):
        coa = server["coa"]
        pyprctl.set_name(f"watchdog-{coa}")
        while not self._terminate.is_set():
            try:
                server["connection_event"].clear()
                connection_state = self.get_connection_state(coa)

                if connection_state == c104.ConnectionState.CLOSED:
                    self.logger.info(f"Attempting to connect to {coa}...")
                    self.connect_server(server)
                elif connection_state == c104.ConnectionState.CLOSED_AWAIT_RECONNECT:
                    self.logger.info(f"Reconnecting to {coa} scheduled...")
                    # self.connect_server(server)
            except Exception as e:
                self.logger.error(f"Error in Watchdog {coa}: {e=}")
                self.logger.error(traceback.format_exc())
            server["connection_event"].wait(self._reconnect_interval_s)
            self._terminate.wait(self._reconnect_interval_s)

    def run(self):
        """
        The clients main loop.
        Monitors the connection states and triggers reconnects if neccesary.
        Works on messages / commands queued for transmission and sends them to the server.
        Continues these tasks until termination is requested, e.g., by the stop method.
        :return:
        """

        while not self._terminate.is_set():
            try:
                # Check for missing connections
                connected = 0
                for coa, server in self._servers.items():
                    if server.get("connection_event") is None:
                        server["connection_event"] = threading.Event()
                    if server.get("watchdog") is None:
                        server["watchdog"] = threading.Thread(name=f"{coa}-watchdog", target=self.server_watchdog, args=(server, ))
                        server["watchdog"].start()

                    if self._terminate.is_set():
                        break
                    if self._is_server_connected(server):
                        connected += 1

                if not self.connected_event.is_set() and connected == len(self._servers):
                    self.connected_event.set()

                # Check for unknown datapoints
                # Iterate Stations
                try:
                    item = self._send_queue.get(True, CLIENT_UPDATE_INTERVAL_S)
                except Empty:
                    continue
                coa = item[0]
                ioa = item[1]
                cot = item[2]
                if not self.has_datapoint(coa, ioa):
                    self.logger.error(
                        f"Sending of IOA {ioa} to station {coa} is not possible: "
                        f"IOA does not exist")
                else:
                    point = self.get_datapoint(coa, ioa, False)
                    self.logger.info(f"Sending Point {point} ({point.value=})")
                    with self._cb_lock:
                        success = point.transmit(cause=cot)
                        self.logger.info(f"Point Sent: {point=} {success=}")
                        self.on_explicit_control_exit(coa, point, success, cot)
            except RuntimeError as e:
                # not sure which Exceptions 'validly' come here, so just re-throw for now
                # raise e
                self.logger.warn(repr(e))
        self.logger.info("Shutting down")
        self._client.disconnect_all()
        for coa, server in self._servers.items():
            if isinstance(server.get("watchdog"), th.Thread):
                server["connection_event"].set()
                server.get("watchdog").join()

    def has_datapoint(self, coa: int, ioa: int) -> bool:
        """
        Checks whether this client knows a certain data point identified by COA and IOA.
        :param coa: The COA of the station associated with the data point
        :param ioa: The (station-unique) IOA of the data point
        :return: True iff the data point is known to the client, False otherwise
        """
        if not self.has_server(coa):
            self.logger.warning("server is none")
            return False
        server = self._get_server(coa)
        return str(ioa) in server["datapoints"]

    def get_datapoint(self, coa: int, ioa: int, as_dict: bool = True) -> Union[C104Point, dict]:
        """
        Returns the data point (as object or dict) identified by COA and IOA.

        :param coa: The COA of the station responsible for the data point.
        :param ioa: The IOA of the data point.
        :param as_dict: Whether to return the point as a dict (if False, a C104Point is returned)
        :return: The data point.
        """
        if not self.has_datapoint(coa, ioa):
            raise ValueError(f"Doesn't have datapoint with coa:ioa {coa}:{ioa}")
        with self._cb_lock:
            server = self._get_server(coa)
            if not str(ioa) in server["datapoints"]:
                raise ValueError(f"Doesn't have datapoint with coa:ioa {coa}:{ioa}")
            dp = server["datapoints"][str(ioa)]
            if as_dict:
                return dp.translate()
            else:
                return dp

    def on_explicit_control_exit(self, coa: int, p: C104Point, success: bool,
                                 orig_cot: c104.Cot) -> None:
        """ Manually put into place, naming the same for style """
        with self._cb_lock:
            try:
                if self.callbacks['on_explicit_control_exit'] is not None:
                    self.callbacks['on_explicit_control_exit'](coa, p, success, orig_cot)
            except Exception as e:
                self.logger.critical(f"on_explicit_control_exit Error: {e=}")

    def update_datapoint(self, coa: int, ioa: int, value) -> bool:
        """
        Updates the value of the given data point
        :param coa: The COA of the station responsible for the data point
        :param ioa: The IOA of the data point
        :param value: The new value to be assigned to the data point
        :return: True iff the new value has been set, False otherwise
        """
        with self._cb_lock:
            server = self._get_server(coa)
            if server is None:
                return False
            if not str(ioa) in server["datapoints"]:
                return False
            server["datapoints"][str(ioa)].value = value
            return True

    def _on_receive_datapoint(self, point: c104.Point, previous_info: c104.Information,
                              message: c104.IncomingMessage) -> c104.ResponseState:
        # TODO: What happens during on_receive on client-side if return False?!
        with self._cb_lock:
            try:
                if self.callbacks['on_receive_datapoint'] is not None:
                    p = C104Point(point)
                    previous_info = C104Point.parse_to_previous_point(previous_info, point)
                    success = self.callbacks['on_receive_datapoint'](p, previous_info, message)
                    return c104.ResponseState.SUCCESS if success else c104.ResponseState.FAILURE
                return c104.ResponseState.NONE
            except Exception as e:
                self.logger.error(f"{e=}")
                self.logger.error(traceback.format_exc())
                return c104.ResponseState.NONE

    def _get_server(self, server: Union[str, int, dict]):
        if type(server) == str or type(server) == int:
            if not self.has_server(server):
                raise ValueError(f"Doesn't have server with ID {server}")
            return self._servers[str(server)]
        return server

    def find_server_by_ip(self, ip: str) -> Optional[dict]:
        for coa, server in self._servers.items():
            if server["ip"] == ip:
                return server
        return None

    @property
    def coa(self) -> int:
        return self._client.originator_address

    def _on_new_station(self, client: c104.Client, connection: c104.Connection, common_address: int) -> None:
        with self._cb_lock:
            self.logger.info(f"New Station: {common_address}")

    def _on_new_point(self, client: c104.Client, station: c104.Station, io_address: int, point_type: c104.Type) -> None:
        #self.logger.info(f"New Point: {station.common_address}.{io_address} - Acquire Lock")
        with self._cb_lock:
            try:
                point = station.add_point(io_address=io_address, type=point_type)
                point.on_receive(callable=self._on_receive_datapoint)
                server = self._get_server(station.common_address)
                c_point = C104Point(point)
                server["datapoints"][str(io_address)] = c_point
            except Exception as e:
                self.logger.error(f"{e=}")

    def bind(self):
        pass

    def send_C_CI(self, coa: int) -> bool:
        target_cons = self._select_matching_connections(coa)
        success = True
        for conn in target_cons:
            success &= conn.counter_interrogation(common_address=coa, cause=c104.Cot.ACTIVATION,
                                                  qualifier=c104.Qoi.STATION, wait_for_response=False)
        return success

    def send_C_CS(self, coa: int):
        target_cons = self._select_matching_connections(coa)
        success = True
        for conn in target_cons:
            success &= conn.interrogation(common_address=coa, cause=c104.Cot.ACTIVATION,
                                          qualifier=c104.Qoi.STATION, wait_for_response=False)
        return success

    def send_C_IC(self, coa: int):
        target_cons = self._select_matching_connections(coa)
        success = True
        for conn in target_cons:
            success &= conn.interrogation(common_address=coa, cause=c104.Cot.ACTIVATION,
                                          qualifier=c104.Qoi.STATION, wait_for_response=False)
        return success

    def send_C_RP(self, coa: int):
        raise NotImplementedError()

    def send_P_AC(self, coa: int, ioa: int, cot: int, qpa: int = 3) -> bool:
        raise NotImplementedError()

    def send_P_ME(self, type_id: int, coa: int, ioa: int, val):
        raise NotImplementedError()

    def _select_matching_connections(self, coa: int) -> List[c104.Connection]:
        if coa == GLOBAL_COA:
            target_cons = [server['connection'] for server in self._servers.values()]
        elif not self.has_server(coa):
            target_cons = []
        else:
            target_cons = [self._get_server(coa)['connection']]
        return target_cons

    def _update_conn_state_if_interro_APDU(self, apdu: APDU, rtu_coa: int):
        if isinstance(apdu, I_FORMAT) and apdu.type == TypeID.C_IC_NA_1:
            new_state = None
            if apdu.cot == COT.ACTIVATION:
                new_state = ConnectionState.INTERRO_STARTED
            elif apdu.cot == COT.ACTIVATION_TERMINATION:
                new_state = ConnectionState.INTERRO_DONE
            if new_state is not None:
                # aodu.coa may be GLOBAL_COA
                server = self._get_server(rtu_coa)
                server['state'] = new_state

    def _is_server_connected(self, server: dict) -> bool:
        if server["connection"] is None:
            return False
        return server.get("connected", server["connection"].is_connected)
