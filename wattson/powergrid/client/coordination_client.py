#!/usr/bin/env python3
import datetime
import logging
import queue
import random
import threading
import time
from typing import Any, Callable, Union, Optional, List, Tuple

import pandapower

from wattson.analysis.statistics.client.statistic_client import StatisticClient
from wattson.powergrid.messages.publish_message import PublishMessage
from wattson.powergrid.messages.request_response_message import RequestResponseMessage
from wattson.util import get_zmqipc

import zmq

from wattson.powergrid.common.constants import DEFAULT_COORD_POWER_PORT, DEFAULT_COORD_IP_ADDR, TEST_PREFIX, \
    ERROR_PREFIX, \
    DEFAULT_COORD_GLOBAL_EVENT_PORT, VALUE_UNKNW, ListOrSingle
from wattson.powergrid.messages import PPQuery, ControlMessageType, TestMessage, ErrorMessage, ControlMessage
from wattson.util.namespace import Namespace


def _prepare_query(table: Union[ListOrSingle[PPQuery], str],
                   column: str = None,
                   index: int = None,
                   value: str = None,
                   log_worthy: bool = True,
                   node_id: Optional[str] = None) -> ListOrSingle[PPQuery]:
    if isinstance(table, PPQuery) or isinstance(table, list):
        if [None] * 3 != [column, index, value]:
            raise RuntimeError(
                "When passing the query directly, don't pass further parameters!")
        query = table
    else:
        if None in [column, index]:
            raise RuntimeError(
                "when not passing the query, pass all required parameters")
        query = PPQuery(table, column, index, value, log_worthy, node_id)
    return query


class CoordinationClient:
    """
    This class is used to send queries to the coordinator for updating or retrieve
    values of the pp net.
    """

    def __init__(self,
                 server_ip: str = None,
                 power_port: int = DEFAULT_COORD_POWER_PORT,
                 global_event_port: int = DEFAULT_COORD_GLOBAL_EVENT_PORT,
                 global_event_handler: Callable[[ControlMessageType], None] = None,
                 logger=None,
                 node_id="NoName",
                 statistics=None,
                 **kwargs):
        self.logger = logging.getLogger(__name__) if logger is None else logger
        self.context = zmq.Context()

        self._on_simulated_time_update_handler = None

        self.statistics: Optional[StatisticClient] = statistics

        self.namespace = kwargs.get("namespace")

        if server_ip is None:
            server_ip = DEFAULT_COORD_IP_ADDR

        self.power_address = "tcp://{}:{}".format(server_ip, power_port)
        ge_address = "tcp://{}:{}".format(server_ip, global_event_port)

        # self.power_address = get_zmqipc(server_ip, power_port)
        # ge_address = get_zmqipc(server_ip, global_event_port)

        self.node_id = node_id
        self._max_connections = threading.BoundedSemaphore(10)
        if global_event_handler is None:
            global_event_handler = self._default_GE_handler
        # self.logger.info(
        #     "NetClient started and will send queries to " + self.power_address)
        self.ge_listener = GlobalEventListener(ge_address, global_event_handler, self._publish_handler, self.namespace)
        self.events: List[ControlMessageType] = []
        self.start_event = threading.Event()
        self._event_events = {}
        self._event_lock = threading.Lock()

        self._subscriptions = {}
        # self.p_ctx = None
        # self.p_socket = None
        # self.lock = threading.Lock()

    def _default_GE_handler(self, t: ControlMessageType):
        self.events.append(t)
        if t == ControlMessageType.start:
            self.start_event.set()
        elif t == ControlMessageType.simtime:
            if self._on_simulated_time_update_handler is not None:
                self._on_simulated_time_update_handler()

    def _publish_handler(self, msg: PublishMessage):
        topic = msg.topic
        data = msg.data
        if topic == "__events__":
            self._event_handler(data)
            return
        for t in ["__all__", topic]:
            if t in self._subscriptions:
                for handler in self._subscriptions[t]:
                    handler(topic, data)

    def subscribe(self, callback: Callable[[str, dict], None], topic: Optional[str] = None):
        if topic is None:
            topic = "__all__"
        if topic not in self._subscriptions:
            self._subscriptions[topic] = []
        self._subscriptions[topic].append(callback)

    def get_subscriptions(self):
        return self._subscriptions

    def unsubscribe(self, topic: str):
        if topic in self._subscriptions:
            del self._subscriptions[topic]

    def set_on_simulated_time_update(self, handler: Callable):
        self._on_simulated_time_update_handler = handler

    def wait_for_start_event(self, timeout=None):
        if self.is_sim_running():
            return
        return self.start_event.wait(timeout=timeout)

    def _event_handler(self, event_data: dict):
        event_name = event_data["event"]
        value = event_data["value"]
        self.logger.debug(f"EVENTS: {event_name} -> {value}")
        with self._event_lock:
            if event_name in self._event_events:
                if value:
                    self._event_events[event_name].set()
                else:
                    self._event_events[event_name].clear()
            else:
                event = threading.Event()
                if value:
                    event.set()
                self._event_events[event_name] = event

    def wait_for_event(self, event_name: str, timeout=None):
        if event_name == "start":
            return self.wait_for_start_event(timeout)
        # Event already occurred?
        self.logger.debug("Getting Event Lock")
        self._event_lock.acquire(True)
        self.logger.debug("Got Event Lock")
        if event_name in self._event_events:
            self.logger.debug(f"Already waiting for {event_name}. Waiting...")
            self._event_lock.release()
            return self._event_events[event_name].wait(timeout)
        # Manual check
        self.logger.debug(f"Manual check for {event_name}...")
        if self.has_event_occurred(event_name):
            self._event_lock.release()
            return True
        # Active waiting (blocking)
        self.logger.debug(f"Actively blocking for {event_name}...")
        event = threading.Event()
        self._event_events[event_name] = event
        self._event_lock.release()
        return event.wait(timeout)

    def start(self):
        self.ge_listener.start()

    def check_connection(self, timeout: Optional[float] = None, legacy=False, register=True) -> bool:
        """
        Send a test command to the net server and check if the net server
        responds within the give timeout. If so, returns True, otherwise return
        False.
        :param timeout: time in seconds to wait for a responds. defaults to None
        (wait until connection is established)
        :type timeout: float
        :param legacy: use legacy implementation of test_messages (just byte
        strings)
        :return: True if connection is valid, False if not
        """
        res = False
        nonce = random.randint(0, 10240)
        qu: queue.Queue = queue.Queue()
        q: Union[bytes, TestMessage]
        if legacy:
            q = TEST_PREFIX + str(nonce).encode()
        else:
            q = TestMessage(self.node_id, register)

        def f():
            tmp = self._send_query(q, raise_exceptions=False)
            qu.put(tmp)

        send_thread = threading.Thread(target=f)

        try:
            send_thread.start()
            resp = qu.get(timeout=timeout)
            res = resp == q
        except:
            res = False
        # finally:
        #     send_thread.join()
        return res

    def _log_duration(self, name, duration):
        if self.statistics is not None:
            self.statistics.log(name, "coordination_roundtrip", value=duration)

    def update_value(self, table: Union[ListOrSingle[PPQuery], str], column: str = None,
                     index: int = None, value: str = None, log_worthy: bool = True):
        query = _prepare_query(table, column, index, value, log_worthy, self.node_id)
        self.logger.debug("Sending update query: " + str(query))
        _start_time = time.time_ns() / 1000 ** 3

        if self.statistics is not None:
            self.statistics.log(event_name=f"{self.node_id}", event_class="coordination", value="update.request")
        response = self._send_query(query)
        if self.statistics is not None:
            self.statistics.log(event_name=f"{self.node_id}", event_class="coordination", value="update.response")

        _end_time = time.time_ns() / 1000 ** 3
        _duration = _end_time - _start_time
        # convert statistics without duration
        self._log_duration("update.rtt", _duration)
        return response

    def retrieve_value(self, table: Union[ListOrSingle[PPQuery], str], column: str = None,
                       index: int = None, log_worthy: bool = True):
        query = _prepare_query(table, column, index, None, log_worthy, self.node_id)
        # self.logger.debug("Sending retrieve query: " + str(query))
        _start_time = time.time_ns() / 1000 ** 3

        if self.statistics is not None:
            self.statistics.log(event_name=f"{self.node_id}", event_class="coordination", value="retrieve.request")
        response = self._send_query(query)
        _end_time = time.time_ns() / 1000 ** 3
        _duration = _end_time - _start_time
        if self.statistics is not None:
            self.statistics.log(event_name=f"{self.node_id}", event_class="coordination", value="retrieve.response")
        self._log_duration("retrieve.rtt", _duration)
        return response

    def _send_query(self, query: ListOrSingle[PPQuery], raise_exceptions=True):
        response = b""
        if self.namespace is not None:
            self.namespace.thread_attach()
        with self._max_connections:
            with self.context.socket(zmq.REQ) as socket:
                socket.connect(self.power_address)
                socket.send_pyobj(query)
                response = socket.recv_pyobj()
        e = None
        if type(response) == bytes and response.startswith(ERROR_PREFIX) \
                or isinstance(response, ErrorMessage):
            if response == VALUE_UNKNW:
                e = RuntimeError(
                    "Coordinator does not know values for query " + str(query))
            else:
                err_str = ""
                if isinstance(response, ErrorMessage):
                    err_str = response.msg
                else:
                    err_str = response.decode()
                e = RuntimeError("Some error occurred: " + err_str)

        if e is not None:
            self.logger.error(e)
            if raise_exceptions:
                raise e
        return response

    def stop(self):
        self.ge_listener.stop()
        self.ge_listener.join()

    def get_powernet(self) -> Optional[pandapower.pandapowerNet]:
        msg = RequestResponseMessage(type="POWERNET")
        resp = self.get_response(msg)
        if resp.is_successful():
            grid = pandapower.from_json_string(resp.response["value"])
            return grid
        return None

    def get_sim_time(self) -> Optional[float]:
        """
        Returns the number of seconds passed since when the coordinator started the simulation.

        This method is sensitive to communication delay in the management network, which should, however, be
        neglectable in most cases. Alternative to `get_sim_time_start` when clock sync is an issue.

        :return: The number of seconds passed since the simulation started.
        """
        msg = RequestResponseMessage(type="SIM_TIME")
        resp = self.get_response(msg)
        if resp.is_successful():
            return resp.response["value"]
        return None

    def get_sim_start_time(self) -> Optional[float]:
        """
        Returns the timestamp of when the coordinator started the simulation.

        If the simulation is not yet running, -1 is returned.

        For a meaningful return value, synchronized clocks are required between the calling host and the coordinator!
        If clocks are not synchronized, consider using `get_sim_time` instead.

        :return: The coordinator's timestamp of the simulation start.
        """
        msg = RequestResponseMessage(type="SIM_START_TIME")
        resp = self.get_response(msg)
        if resp.is_successful():
            return resp.response["value"]
        return None

    def get_simulated_time(self) -> Optional[Tuple[float, float]]:
        """
        Returns the start datetime [0] and the speed [1] of the simulated time as a Tuple.
        :return: [start_timestamp, speed] or None on error
        """
        msg = RequestResponseMessage(type="GET_SIMULATED_TIME_INFO")
        resp = self.get_response(msg)
        if not resp.is_successful():
            return None
        return resp.response["start_time"], resp.response["speed"]

    def set_simulated_time(self, start: Union[datetime.datetime, float], speed: float = 1) -> bool:
        """
        Instructs the coordinator to update and populate the simulated time and simulation speed.
        :param start: The timestamp or datetime when the simulation should start
        :param speed: The speed of the simulation (e.g., 1)
        :return: True iff the update has been accepted by the coordinator
        """
        start_ts = start if type(start) == float else start.timestamp()
        msg = RequestResponseMessage(request={
            "type": "SET_SIMULATED_TIME_INFO",
            "start_time": start_ts,
            "speed": speed
        })
        resp = self.get_response(msg)
        return resp.is_successful()

    def has_event_occurred(self, event_name) -> bool:
        msg = RequestResponseMessage(request={
            "type": "GET_EVENT_OCCURRED",
            "event": event_name
        })
        resp = self.get_response(msg)
        if resp.is_successful():
            return resp.response["value"]
        return False

    def trigger_event(self, event_name) -> bool:
        msg = RequestResponseMessage(request={
            "type": "SET_EVENT_OCCURRED",
            "event": event_name,
            "value": True
        })
        resp = self.get_response(msg)
        return resp.is_successful()

    def is_sim_running(self) -> Optional[bool]:
        msg = RequestResponseMessage(type="SIM_RUNNING")
        resp = self.get_response(msg)
        if resp.is_successful():
            return resp.response["value"]
        return None

    def get_response(self, msg: RequestResponseMessage) -> RequestResponseMessage:
        resp: RequestResponseMessage = self._send_query(msg, False)
        if resp.response is None:
            self.logger.error(f"Got no response for {msg.request['type']}")
            resp.response = {
                "success": False,
                "value": None
            }
        elif not resp.response["success"]:
            self.logger.error(f"Query {msg.request['type']} was not successful")
        return resp

    def request_shutdown(self) -> bool:
        """
        Requests the coordinator to shutdown the simulation.

        :return: True if the coordinator acknowledges the shutdown request.
        """
        msg = RequestResponseMessage(type="SHUTDOWN")
        resp = self.get_response(msg)
        if resp.is_successful():
            return resp.response["value"]
        return False

    @property
    def ge_handler(self):
        return self.ge_listener.ge_handler

    @ge_handler.setter
    def ge_handler(self, handler):
        self.ge_listener.ge_handler = handler


class GlobalEventListener(threading.Thread):
    def __init__(self, server_address: str,
                 ge_handler: Callable[[ControlMessageType], Any],
                 publish_handler: Callable[[PublishMessage], Any],
                 namespace: Optional[Namespace] = None):
        super().__init__()
        self.server_address = server_address
        self.ge_handler = ge_handler
        self.publish_handler = publish_handler
        self._terminate = threading.Event()
        self._poll_time = 1
        self._namespace = namespace

    def run(self):
        if self._namespace is not None:
            self._namespace.thread_attach()
        with zmq.Context() as ctx:
            with ctx.socket(zmq.SUB) as sock:
                sock.connect(self.server_address)
                sock.subscribe(b"")
                while not self._terminate.is_set():
                    if sock.poll(self._poll_time):
                        msg = sock.recv_pyobj()
                        if type(msg) == ControlMessage:
                            self.ge_handler(msg.type)
                        elif type(msg) == PublishMessage:
                            self.publish_handler(msg)

    #                         todo: handle invalid message types
    def stop(self):
        self._terminate.set()
