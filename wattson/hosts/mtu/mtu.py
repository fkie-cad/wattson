import copy
import logging
import threading
import threading as th
import time
from typing import Dict, Tuple, Type, List, Optional, Any, Union
import pathlib

from wattson.analysis.statistics.client.statistic_client import StatisticClient
from wattson.cosimulation.control.interface.wattson_client import WattsonClient
from wattson.cosimulation.control.messages.wattson_event import WattsonEvent
from wattson.util import (
    get_logger, log_contexts,
    UnsupportedError, InvalidIEC104Error
)

import wattson.powergrid
from wattson.apps.interface.servers import PublishingServer
from wattson.apps.interface.servers import CommandServer
import wattson.apps.interface.util.messages as msgs
from wattson.apps.interface.manager import SubscriptionManager

from wattson.iec104.interface.client import IECClientInterface
from wattson.iec104.common import ConnectionState, MTU_UPDATE_INTERVAL_S, MTU_READY_EVENT, SERVER_DEFAULT_PORT
from wattson.iec104.common.datapoint import IEC104Point
from wattson.iec104.common.iec104message import IEC104Message
from wattson.iec104.interface.apdus import APDU, I_FORMAT

import faulthandler, signal


class MTU:
    """
    Initialize a SCADA with given "client" datapoints and IEC Client-class.
    This usually only exists once per grid. Actually, "MTU" is suited better.
    Features of the entire MTU:
    - automatically (re)connect to RTUs
    - primitive sending of commands based on rules
    - perform general interrogation
    - receive and log ASDUs from RTUs
    """

    def __init__(self, client_class: Type[IECClientInterface], datapoints: List, **kwargs):
        """
        :param datapoints: Dataframe that contains information about all IOs
        :param node_id: node_id of the mininet host for this MTU. Assigned by CPS-IDS, relevant to identify this MTU, not relevant for IEC 104/
        :param coord_ip: IP address of the coordinator
        """
        self.node_id = str(kwargs.get("node_id", "mtuX"))
        self.entity_id = kwargs.get("entity_id", "mtuX")
        self.datapoints = datapoints
        self.coa_map = {}
        self.data_point_cache = {}
        self.rtus = kwargs.get("rtus", {})
        statistics_server = kwargs.get("statistics", {}).get("server")

        faulthandler.register(signal.SIGUSR2)

        self.ready_event = threading.Event()
        self._connect_delay_s = kwargs.get("iec104_connect_delay", 0)

        self.logger = get_logger(self.node_id, "Wattson.MTU", level=logging.INFO, use_context_logger=False,
                                 use_basic_logger=False, use_async_logger=True, use_fake_logger=False)
        self.statistics = StatisticClient(ip=statistics_server, host=self.node_id, logger=self.logger)
        self.statistics.start()

        for dp in self.datapoints:
            if dp["protocol"] == "60870-5-104":
                coa = dp["protocol_data"]["coa"]
                if coa not in self.coa_map:
                    for host, node in self.rtus.items():
                        #if int(node["coa"]) == int(coa):
                        self.coa_map[str(coa)] = host
                        break
        # mapping coa -> responsible 104 Client connection
        # available contexts for MTU: setup, on_send, on_receive
        self.active_contexts = {log_contexts.C_CS, log_contexts.C_IC,
                                log_contexts.DP_C, log_contexts.ON_SEND, log_contexts.ON_RECEIVE}

        self.do_general_interrogation = kwargs.get("do_general_interrogation", True)
        self.do_clock_sync = kwargs.get("do_clock_sync", True)
        self._enable_rtu_connection_state_observation = kwargs.get("enable_rtu_connection_state_observation", False)

        self.iec_client = client_class(
            mtu=self,
            datapoints=self.datapoints,
            logger=self.logger.getChild("104Client"),
            on_receive_raw=self.on_receive_apdu,
            on_send_apdu=self.on_send_apdu,
            on_send_raw=self.on_send_apdu,
            on_receive_apdu=self.on_receive_apdu,
            on_receive_datapoint=self.on_receive_datapoint,
            on_receive_control_callback=self.on_explicit_control_exit,
            on_explicit_control_exit=self.on_explicit_control_exit,
            on_connection_change=self.on_connection_change,
            do_clock_sync=self.do_clock_sync,
            do_general_interrogation=self.do_general_interrogation,
            connect_delay_s=self._connect_delay_s
        )
        self.logger.debug(f"IEC Client class: {client_class}")

        # this MTU's IP address
        mtu_ip = "*"
        rtus = sorted([int(node["coa"]) for node in self.rtus.values()])
        if (max_rtus := kwargs.get('max_rtus')) is not None:
            rtus = rtus[:max_rtus]
        self._rtus = sorted(set(rtus))
        self.logger.critical(f'{self._rtus=}')

        self.subscription_manager = SubscriptionManager(self, self._rtus)
        self.publishing_server = PublishingServer(self.logger, ip=mtu_ip)
        self.command_server = CommandServer(self.subscription_manager, ip=mtu_ip)
        self.subscription_manager.add_subscription_servers(self.publishing_server, self.command_server)

        self.logger.info("Will connect to RTUs " + str(self._rtus) + " max_rtus= " + str(max_rtus))
        # per known RTU, build one client connection

        for node_id, node in self.rtus.items():
            coa = int(node["coa"])
            if coa in self._rtus:
                ip = node["ip"]
                port = node.get("port", SERVER_DEFAULT_PORT)
                self.iec_client.add_server(ip, coa, port=port)

        self.notify_coordinator = kwargs.get("notify_coordinator", True)
        self.wattson_client: Optional[WattsonClient] = None
        self.wattson_client_config = kwargs.get("wattson_client_config")
        if self.wattson_client_config is not None:
            self.wattson_client = WattsonClient(
                query_server_socket_string=self.wattson_client_config["query_socket"],
                publish_server_socket_string=self.wattson_client_config["publish_socket"],
                namespace=None,
                client_name=self.entity_id
            )

        self._is_stopped = th.Event()
        # self._watchdog_thread = threading.Thread(target=self._watchdog)

    def __str__(self):
        return f"MTU: {self.data_point_count()} known IOS"

    def coa_to_host(self, coa: Union[int, str]) -> Any:
        """
        Retrieves host-information for a specific RTU

        Args:
            coa: ID of RTU

        Returns:
            Host-info if coa is known to MTU, else None
        """
        if str(coa) in self.coa_map:
            return self.coa_map[str(coa)]
        return None

    def data_point_count(self) -> int:
        """
        Returns:
            Total count of dps known to the MTU
        """
        return len(self.datapoints)
        """sum = 0
        for dps in self.datapoints.values():
            sum += len(dps)
        return sum"""

    def get_rtu_status(self) -> Dict[int, Tuple[str, int, ConnectionState]]:
        """ Checks connection status for all RTUs """
        status = {coa: (self.iec_client.get_server_IP(coa), self.iec_client.get_server_port(coa),
                        self.iec_client.get_wattson_connection_state(coa).name) for coa in self._rtus}
        return status

    def get_cache(self) -> Dict[str, Dict[str, Any]]:
        return copy.deepcopy(self.data_point_cache)

    def _watchdog(self):
        while not self._is_stopped.is_set():
            self.logger.info(f"Still alive...")
            time.sleep(10)
        self.logger.info(f"Stopping Watchdog")

    def cast_datapoints(self):
        """ TODO: add info like writeable, ACT-Status, etc.  to datapoints"""
        cast_dp = {}
        return cast_dp

    def start(self):
        if self.wattson_client is not None:
            self.wattson_client.start()
            self.wattson_client.require_connection()
            self.logger.info(f"Registering to WattsonServer with {self.entity_id=}")
            if not self.wattson_client.register():
                self.logger.warning("Could not register to WattsonServer")
            self.logger.info(f"Waiting for start event")
            self.wattson_client.event_wait(WattsonEvent.START)
            self.logger.info("Received Start Event")
        else:
            self.logger.info("No coordinator attached, skipping timing synchronisation")

        # self._watchdog_thread.start()

        self.logger.info(f"starting Subscription Manager on IP {self.publishing_server.server_address}")
        self.subscription_manager.start()

        self.logger.debug("starting command server")
        self.command_server.start()
        self.logger.debug("starting interface server")
        self.publishing_server.start()
        self.logger.debug("starting read-subs")

        self.logger.info("Starting IEC104 Client")
        self.iec_client.start()

        if self.wattson_client is not None:
            connected = False
            while not connected and not self._is_stopped.is_set():
                connected = self.iec_client.wait_for_connection()
            if connected and self.notify_coordinator:
                self.logger.info("Triggering MTU_READY_EVENT")
                self.wattson_client.event_set(MTU_READY_EVENT)

        self.ready_event.set()

        while not self._is_stopped.is_set():
            time.sleep(MTU_UPDATE_INTERVAL_S)

    def on_explicit_control_exit(self, coa: int, point: IEC104Point, success: bool, orig_cot: int):
        """
        Callback for when a set-cmd was send to a point and we received a reply

        Args:
            coa: ID of RTU the cmd was send to
            point: dp with the status the RTU's dp should be set to
            success: True iff the RTU accepted and performed the resp. update
            orig_cot: Cause of transmission set by the Master

        Returns:
            None
        """
        self.subscription_manager.on_explicit_control_exit(coa, point, success, orig_cot)

    def on_receive_datapoint(self, p: IEC104Point, old_point: IEC104Point,
                             incoming_message: IEC104Message):
        """
        Callback for any individual newly incoming datapoint

        Args:
            p: newly incoming datapoint data
            old_point: previous dp status
            incoming_message: further protocol-information (header,...)

        Returns:
            None
        """
        self.data_point_cache[p.coa_ioa_str()] = {
            "cot": incoming_message.cot.value,
            "value": p.value,
            "time": time.time() * 1000
        }

        self.subscription_manager.on_datapoint(p, old_point, incoming_message)

    def on_receive_apdu(self, apdu: APDU, coa: int = -1, raw_callback: bool = False):
        """
        Callback for any individual newly incoming L5-data

        Args:
            apdu: incoming data
            coa: ID of RTU sending this data
            raw_callback: if the data has been reconstructed through a "raw-data" callback method

        Returns:
            None
        """
        if isinstance(apdu, I_FORMAT):
            if apdu.type < 45:    # READ
                self.statistics.log(event_name=str(coa), event_class="apdu.monitoring.response", value="receive")
            else:
                self.statistics.log(event_name=str(coa), event_class="apdu.control.response", value="receive")

        self.logger.debug(apdu)
        self.statistics.log(event_name=str(coa), event_class="apdu.monitoring", value="receive")
        try:
            self.subscription_manager.on_receive_apdu(apdu, raw_callback, coa=coa)
            pass
        except (UnsupportedError, InvalidIEC104Error) as e:
            # TODO: Notify subscribers of errors?
            self.logger.warning(f"Ignoring rcvd APDU for subMgr: {apdu}. \n{e}")

    def on_send_apdu(self, apdu: APDU, rtu_coa: int):
        """
        Callback for any L5-data just send out

        Args:
            apdu: data send
            rtu_coa: ID of RTU it was send to

        Returns:
            None
        """
        if isinstance(apdu, I_FORMAT):
            if apdu.type == 102:    # READ
                self.statistics.log(str(rtu_coa), event_class="apdu.monitoring.request", value="send")
            else:
                self.statistics.log(str(rtu_coa), event_class="apdu.control.request", value="send")

        self.logger.debug(f"send (to {rtu_coa}): {apdu}")
        self.statistics.log(str(rtu_coa), event_class="apdu.control", value="send")
        try:
            self.subscription_manager.on_send_apdu(apdu, rtu_coa)
            pass
        except (UnsupportedError, InvalidIEC104Error) as e:
            # TODO: notify subscribers of errors?
            self.logger.warning(f"Ignoring send APDU for subMgr {apdu}. \n{e}")

    def on_connection_change(self, coa: int, connected: bool, ip: str, port: int):
        """
        Callback for when an RTU changes its connection status.

        Args:
            coa: ID of RTU
            connected: True if L5-connection is now established
            ip: IP for connected RTU
            port: server-port of the protocol expected for this RTU

        Returns:
            None
        """
        if not connected:
            # self.logger.warning(f"No longer connected to RTU {coa}")
            self._notify_rtu_connection_state(str(coa), "connection_lost")
        else:
            # self.logger.info(f"Connected to RTU {coa}")
            self._notify_rtu_connection_state(str(coa), "connection_established")
        self.subscription_manager.on_connection_change(coa, connected, ip, port)

    def _notify_rtu_connection_state(self, rtu: str, event_type: str):
        if self.wattson_client is not None and self._enable_rtu_connection_state_observation:
            from wattson.cosimulation.control.messages.wattson_notification import WattsonNotification
            from wattson.cosimulation.simulators.network.messages.wattson_network_notificaction_topics import WattsonNetworkNotificationTopic
            notification = WattsonNotification(
                    notification_topic=WattsonNetworkNotificationTopic.NODE_CUSTOM_EVENT,
                    notification_data={
                        "event_context": "mtu",
                        "event_type": event_type,
                        "rtu_entity_id": str(rtu)
                    }
                )
            notification.recipients = ["*"]
            self.wattson_client.notify(notification=notification)

    def stop(self):
        """
        Stop everything.
        """
        if not self._is_stopped.is_set():
            self.logger.info(f"Stopping MTU")
            self._is_stopped.set()

            self.iec_client.stop()
            self.subscription_manager.stop()
            self.command_server.stop()
            self.publishing_server.stop()
            self.statistics.stop()
            if self.wattson_client is not None:
                self.wattson_client.stop(0)

            #if self._watchdog_thread is not None:
            #    self.logger.info(f"  Waiting for Watchdog")
            #    self._watchdog_thread.join(10)
            self.logger.info(f"  Waiting for IECClient")
            if hasattr(self.iec_client, "join"):
                self.iec_client.join()
            self.logger.info(f"  Waiting for Subscription Manager")
            self.subscription_manager.join()
            self.logger.info(f"  Waiting for CommandServer")
            self.command_server.join()
            self.logger.info(f"  Waiting for PublishingServer")
            self.publishing_server.join()
            self.logger.info(f"  Waiting for StatisticsClient")
            self.statistics.join()
            if self.wattson_client is not None:
                self.logger.info(f"  Waiting for Wattson    Client")
                self.wattson_client.join()

    def get_single_RTU_conn_status(self, rtu_id: int) -> ConnectionState:
        return self.iec_client.get_wattson_connection_state(rtu_id)

    @property
    def subscription_policy(self):
        return self.subscription_manager.subscription_policy

    @subscription_policy.setter
    def subscription_policy(self, new_policy: Dict[str, bool]):
        self.subscription_manager.subscription_policy = new_policy

    def wait_until_ready(self, timeout: Optional[float] = None):
        self.ready_event.wait(timeout=timeout)
