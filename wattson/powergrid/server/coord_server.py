#!/usr/bin/env python3
import datetime
import os
import signal
import threading
import time
from pathlib import Path
from typing import Union, List, Optional
import importlib.util

import numpy as np
import pytz

import wattson.util
import pandapower
import zmq
import zmq.sugar

from wattson.analysis.statistics.client.statistic_client import StatisticClient
from wattson.powergrid.common.constants import *
from wattson.powergrid.common.events import ELEMENT_UPDATED, PROFILES_READY
from wattson.powergrid.messages import PPQuery, ControlMessage, ControlMessageType, \
    CoordinationMessage, TestMessage, ErrorMessage
from wattson.powergrid.messages.publish_message import PublishMessage
from wattson.powergrid.messages.request_response_message import RequestResponseMessage
from wattson.powergrid.server.power_simulator import PowerSimulator
from wattson.powergrid.server.publisher import Publisher
from wattson.util.powernet import sanitize_power_net


class CoordinationServer(threading.Thread):
    """
    This class couples the simulation of a pp net with communication (here: a
    zmq server).
    """

    def __init__(self, net: pandapower.pandapowerNet,
                 scenario_path: Path,
                 ups: float = 1.0,
                 ip_address: str = DEFAULT_COORD_IP_ADDR,
                 port: int = DEFAULT_COORD_POWER_PORT,
                 publisher_port: int = DEFAULT_COORD_GLOBAL_EVENT_PORT,
                 nodes: List[str] = None,
                 coas: List[str] = None,
                 max_wait_time_s: int = 10,
                 grid_config: Union[bool, str] = False,
                 config: dict = None,
                 main_pid: int = -1,
                 profile_loader_exists: bool = False,
                 statistics: Optional[dict] = None):

        super().__init__()
        self.logger = wattson.util.get_logger("coord", "Wattson.Coordinator", use_context_logger=False)
        # Remove profiles, ...
        sanitize_power_net(net)

        self._deployment_completed_event = threading.Event()
        self.logger.info(f"Waiting for Wattson Deployment to complete (at PID {os.getpid()})")
        signal.signal(signal.SIGUSR1, self._deployment_completed)
        self.subscribed_elements = set()
        self.profile_loader_exists = profile_loader_exists
        self.config = config
        self.main_pid = main_pid  # The PID of the Wattson main process (if any)
        self.scenario_path: Path = scenario_path
        self.grid_config = grid_config
        if isinstance(self.grid_config, str):
            config_path = self.scenario_path.joinpath(self.grid_config)
            if not config_path.exists():
                self.logger.warning(f"Grid Config Path {self.grid_config} does not exist relative to scenario")
                self.logger.warning(f"Full path: {config_path}")
            else:
                spec = importlib.util.spec_from_file_location("grid.configurator", config_path)
                grid_configurator = importlib.util.module_from_spec(spec)
                self.logger.info(f"Applying Configuration changes to grid from {self.grid_config}")
                spec.loader.exec_module(grid_configurator)
                net = grid_configurator.configure_grid(net)

        self.power_simulator = PowerSimulator(self, net, ups, config=self.config)
        #self.server_address = wattson.util.get_zmqipc(ip_address, port)
        self.server_address = f"tcp://{ip_address}:{port}"

        self._events = {}

        statistics_ip = statistics.get("server")
        self.statistics = StatisticClient(ip=statistics_ip,
                                          host="coord",
                                          logger=self.logger)
        self.statistics.start()

        self._terminate = threading.Event()
        self._running = threading.Event()
        self._sim_start_time = -1

        self._simulated_time_start = time.time()
        if "simulated_time_start" in self.config:
            time_str = self.config["simulated_time_start"]
            self._simulated_time_start = datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")\
                .replace(tzinfo=pytz.UTC).timestamp()
        self._simulated_time_speed = self.config.get("simulated_time_speed", 1)

        # mapping node-id -> state (available or not)
        if nodes is None or len(nodes) == 0:
            self.nodes = {}
            self.logger.warning("Coordinator does not know any nodes!")
        else:
            self.nodes = {str(n): False for n in nodes}

        #publ_address = wattson.util.get_zmqipc(ip_address, publisher_port)
        publ_address = f"tcp://{ip_address}:{publisher_port}"
        self.publisher = Publisher(publ_address)

        self.i = 0
        self._start_event_sent = threading.Event()
        self.query_not_blocked_for_power_flow = threading.Event()
        if not self.profile_loader_exists:
            self._events[PROFILES_READY] = True
            self.query_not_blocked_for_power_flow.set()
        self._wait_time = None
        self._max_wait_time = max_wait_time_s

    def _deployment_completed(self, num=None, frame=None):
        self.logger.info("Wattson deployment completed")
        self._deployment_completed_event.set()

    def get_current_simulated_time(self) -> datetime.datetime:
        simulated_time_passed = self.get_elapsed_simulated_time()
        simulated_timestamp = self._simulated_time_start + simulated_time_passed
        return datetime.datetime.fromtimestamp(simulated_timestamp, tz=pytz.UTC)

    def get_elapsed_simulated_time(self) -> float:
        real_time_passed = time.time() - self._sim_start_time
        return real_time_passed * self._simulated_time_speed

    def get_simulated_speed(self) -> float:
        return self._simulated_time_speed

    def run(self):
        self._wait_time = None
        self.power_simulator.run()
        self.publisher.start()

        with zmq.Context() as context:
            with context.socket(zmq.REP) as sock:
                sock.bind(self.server_address)
                self.logger.info(f"Coordinator launched and is listening for queries on: {self.server_address}")
                self.logger.info(f"Waiting for nodes: {' '.join(self.nodes.keys())} to connect")
                self._running.set()
                while not self._terminate.is_set():
                    first_i = self.i

                    if self._deployment_completed_event.is_set() and not self._start_event_sent.is_set():
                        if self._wait_time is None:
                            self._wait_time = time.time()
                        if time.time() - self._wait_time > self._max_wait_time:
                            self.logger.warning("Timeout on client connection wait. Forcing start now.")
                            self.notify_start()

                    if sock.poll(1000):
                        query: Union[bytes, CoordinationMessage, List[PPQuery]] = sock.recv_pyobj()
                    else:
                        continue

                    self.logger.debug("received query " + str(query))
                    if isinstance(query, PPQuery) or isinstance(query, list) and isinstance(query[0], PPQuery):
                        res = self.handle_PPQuery(query)
                    elif isinstance(query, TestMessage):
                        res = self.handle_testmsg(query, sock)
                    elif isinstance(query, RequestResponseMessage):
                        res = self.handle_rr_message(query, sock)
                    elif isinstance(query, bytes) and query.startswith(TEST_PREFIX):
                        res = self.handle_legacy_testmsg(query)
                    else:
                        self.logger.error("Error: query has invalid format")
                        res = QUERY_INVALID

                    if res is not None:
                        if isinstance(res, List) and len(res) == 1:
                            res = res[0]
                        sock.send_pyobj(res)
                        last_i = self.i
                        if last_i - first_i == 1:
                            self.logger.debug("Response {} for query {} sent".format(res, first_i))
                            self.logger.debug(f"Full Query: {query}")
                        else:
                            self.logger.debug(f"Response for batch query {first_i}...{last_i} sent!")
                    self.i = (self.i) % 4096
            self._running.clear()

    def stop(self):
        self.logger.info("Stopping Coordinator")
        self._terminate.set()
        self._start_event_sent.clear()
        self.power_simulator.stop()
        self.logger.info("Internal Power Simulation has stopped")
        self.publisher.stop()
        self.publisher.join()
        self.logger.info("Publisher has stopped")
        self.statistics.stop()
        self.statistics.join()
        self.logger.info("Statistics have stopped")
        self.logger.info("Coordinator stopped!")

    def handle_PPQuery(self, queries: ListOrSingle[PPQuery]) -> Union[bytes, ListOrSingle[Union[PPQuery, ErrorMessage]]]:
        raw_res = None
        resp = []
        if isinstance(queries, PPQuery):
            queries = [queries]
        try:
            for query in queries:
                i = self.i
                node_id = query.node_id
                try:
                    if query.value is None:
                        try:
                            if node_id:
                                self.statistics.log(event_name=f"{node_id}", event_class="coordination",
                                                    value="retrieve.request")
                            self.query_not_blocked_for_power_flow.wait()
                            res = self.power_simulator.answer_retrieval_query(query)
                            self.logger.debug(f"Query {query} result: {res}")

                            if node_id:
                                self.statistics.log(event_name=f"{node_id}", event_class="coordination",
                                                    value="retrieve.response")
                        except (KeyError, IndexError) as e:
                            self.logger.error(
                                "unable to find answer for query {}, "
                                "returning nan as value. e: {}".format(
                                    query, e))
                            res = np.nan  # self.logger.debug("Sending response for Query {}: {}".format(query, res))
                    else:
                        if query.log_worthy:
                            self.logger.info(f"Update: {query}")
                        # this is an updating query
                        if node_id:
                            self.statistics.log(event_name=f"{node_id}", event_class="coordination", value="update.request")
                        self.power_simulator.add_update_query(query)
                        if node_id:
                            self.statistics.log(event_name=f"{node_id}", event_class="coordination", value="update.response")

                        res = query
                except KeyError as e:
                    res = ErrorMessage(e, "datapoint not available")
                    # raw_res = VALUE_UNKNW
                    self.logger.warning(f"Received query {i} for unavailable datapoint")
                self.i += 1
                resp.append(res)
        except Exception as e:
            self.logger.error("Exception after communication with the simulator")
            raw_res = COMM_SIM_FAILED + (":" + str(e)).encode()
            self.logger.exception(e)

        if raw_res is not None:
            return raw_res
        else:
            return resp

    def handle_testmsg(self, msg: TestMessage, sock: zmq.sugar.Socket) -> Union[None, TestMessage, ErrorMessage]:
        """
        Handle a "new" TestMessage. Caution: if this is the last test message,
         the response is sent directly and None is returned. A global event
         to indicate the start of the co-sim is sent via the pub/sub socket.
        :param msg:
        :return:
        """
        resp: Union[ErrorMessage, TestMessage, None] = msg
        node_id = msg.node_id
        #node_id = self.coa_map[coa]
        if not msg.register:
            self.logger.info(f"node {node_id} connect but does not 'register'")
        elif node_id not in self.nodes:
            #resp = None
            self.logger.info(f"Unknown node {node_id} registered")
        #elif self.nodes[node_id]:
        #    resp = ErrorMsg(msg="Node has been registered before!")
        else:
            self.nodes[node_id] = True
            no_registered_nodes = list(self.nodes.values()).count(True)
            self.logger.info(f"node {node_id} has registered, {no_registered_nodes}/{len(self.nodes)} are reg.")
            self.logger.debug(f"Missing nodes: {[node for node in self.nodes if not self.nodes[node]]}")
            if self.test_registration_completed():
                resp = None
                sock.send_pyobj(msg)
                time.sleep(1)
                self.notify_start()

        return resp

    def test_registration_completed(self) -> bool:
        no_registered_nodes = list(self.nodes.values()).count(True)
        if len(self.nodes) <= no_registered_nodes and not self._start_event_sent.is_set():
            if self._events.get(PROFILES_READY, False):
                return True
        return False

    def handle_legacy_testmsg(self, msg):
        return msg

    def handle_rr_message(self, msg, sock):
        req = msg.request
        if req["type"] == "SIM_TIME":
            if self._start_event_sent.is_set():
                msg.answer(time.time() - self._sim_start_time)
            else:
                msg.answer(-1)
        elif req["type"] == "SIM_START_TIME":
            self.logger.info(f"Sim-Start-Time requested. Returning {self._sim_start_time}")
            if self._start_event_sent.is_set():
                msg.answer(self._sim_start_time)
            else:
                msg.answer(-1)
        elif req["type"] == "SIM_RUNNING":
            msg.answer(self._start_event_sent.is_set())
        elif req["type"] == "SHUTDOWN":
            msg.answer(self.main_pid != -1)
            if self.main_pid != -1:
                self.logger.info(f"Sending SIGUSR1 to Wattson main process {self.main_pid}")
                os.kill(self.main_pid, signal.SIGUSR1)
            else:
                self.logger.info(f"No Wattson main process known")
        elif req["type"] == "POWERNET":
            grid = self.power_simulator.get_powernet()
            grid_json = pandapower.to_json(grid)
            msg.answer(grid_json)
        elif req["type"] == "SET_SIMULATED_TIME_INFO":
            self._simulated_time_start = req.get("start_time", self._simulated_time_start)
            self._simulated_time_speed = req.get("speed", self._simulated_time_speed)
            dt = datetime.datetime.fromtimestamp(self._simulated_time_start)
            self.logger.info(f"Updating simulated start time to {str(dt)} at speed {self._simulated_time_speed}")
            msg.ok()
            # Notify all clients that speed and start time have been updated
            self.publisher.send_msg(ControlMessage(ControlMessageType.simtime))
        elif req["type"] == "GET_SIMULATED_TIME_INFO":
            msg.answer({
                "start_time": self._simulated_time_start,
                "speed": self._simulated_time_speed
            })
        elif req["type"] == "PUBLISH_EVENT":
            data = req.get("data")
            topic = req.get("topic")
            if self.publish(data, topic):
                msg.ok()
            else:
                msg.fail()
        elif req["type"] == "SET_EVENT_OCCURRED":
            event = req.get("event")
            value = bool(req.get("value"))
            self._events[event] = value
            self._event_updated(event)
            self.publish({
                "event": event,
                "value": value
            }, "__events__")
            msg.ok()
        elif req["type"] == "GET_EVENT_OCCURRED":
            event = req.get("event")
            resp = self._events.get(event, False)
            self.logger.info(f"GET_EVENT_OCCURRED: {event} -> {resp}")
            msg.answer({
                "value": resp
            })
        elif req["type"] == "SUBSCRIBE_ELEMENT_UPDATE":
            elements = req.get("elements")
            if elements is None:
                elements = set()
                elements.add("*")
            self.subscribed_elements.update(elements)
            #self.logger.info(f"SUBSCRIBE_ELEMENT_UPDATE: {self.subscribed_elements}")
            msg.ok()
        else:
            self.logger.warning(f"Invalid Request: {req['type']}")
            msg.fail()

        return msg

    def publish(self, message: Union[dict, list, PublishMessage], topic: Optional[str] = None):
        if type(message) == dict or type(message) == list:
            if topic is None:
                return False
            message = PublishMessage(message, topic)
        self.publisher.send_msg(message)
        return True

    def notify_start(self):
        """
        Notify all nodes to start operating.
        :return:
        """
        self.logger.info("Sending global START event")
        self._sim_start_time = time.time()
        if "simulated_time_start" not in self.config:
            self._simulated_time_start = time.time()
            self.publisher.send_msg(ControlMessage(ControlMessageType.simtime))
        self.statistics.log("run_state", event_class="simulation", timestamp=self._sim_start_time, value="started")
        self.publisher.send_msg(ControlMessage(ControlMessageType.start))
        self.logger.info(f"SIM-TIME-MAPPING !! REALTIME !! {self._sim_start_time} !! SIMTIME "
                         f"!! {self._simulated_time_start} !! SPEED !! {self._simulated_time_speed}")
        self._start_event_sent.set()

    def notify_powerflow_completed(self):
        self.query_not_blocked_for_power_flow.set()
        self.publisher.send_msg(ControlMessage(ControlMessageType.update))

    def notify_element_updated(self, table, column, index, value):
        if f"{table}.{index}.{column}" in self.subscribed_elements or "*" in self.subscribed_elements:
            data = {
                "table": table,
                "column": column,
                "index": index,
                "value": value
            }
            self.publish(data, ELEMENT_UPDATED)


    def _event_updated(self, event):
        if event == PROFILES_READY:
            self.query_not_blocked_for_power_flow.clear()
            self.logger.info(f"Profile Loader set first iteration - Event set to True")
            if self.test_registration_completed():
                self.notify_start()
