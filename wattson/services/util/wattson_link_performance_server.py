import json
import math
import select
import threading
import time
from pathlib import Path
from typing import Dict, Optional

from wattson.cosimulation.control.interface.wattson_client import WattsonClient
from wattson.cosimulation.control.messages.wattson_notification import WattsonNotification
from wattson.cosimulation.simulators.network.messages.wattson_network_notificaction_topics import WattsonNetworkNotificationTopic
from wattson.services.deployment import PythonDeployment
import socket

from wattson.util import get_logger


class WattsonLinkPerformanceServer(PythonDeployment):
    def __init__(self, configuration: Dict):
        super().__init__(configuration)
        self._ip = self.config.get("ip", "127.0.0.1")
        self._port = self.config.get("port", 20001)
        self._buffer_size = self.config.get("buffer_size", 1024)
        self._payload_size = self.config.get("payload_size", 1024)
        self._expected_interval_ms = self.config.get("expected_interval_ms", 10)

        self._wattson_client_config = self.config.get("wattson_client_config")
        self._wattson_client: Optional[WattsonClient] = None
        if self._wattson_client_config is not None:
            self._wattson_client = WattsonClient(query_server_socket_string=self._wattson_client_config["query_socket"],
                                                 publish_server_socket_string=self._wattson_client_config["publish_socket"],
                                                 client_name=self.__class__.__name__)

        self._work_dir = self.config.get("workdir", Path("."))
        self._result_file_name = self.config.get("result_file", "results.json")

        self.logger = get_logger("WattsonLinkPerformanceServer", "WattsonLinkPerformanceServer")
        self._terminate_requested = threading.Event()
        self._measurement_thread: Optional[threading.Thread] = None
        self._probes = {}
        self._events = []
        self._start_timestamp_ns = 0

    def start(self):
        self.logger.info("Starting measurement server thread")
        self._terminate_requested.clear()
        if self._wattson_client is not None:
            self._wattson_client.register()
            self._wattson_client.subscribe(WattsonNetworkNotificationTopic.LINK_PROPERTY_CHANGED, self._log_event)
            self._wattson_client.subscribe("wattson-performance-server-marker", self._log_event)
        self._measurement_thread = threading.Thread(target=self._measurement_runner)
        self._measurement_thread.start()
        self._measurement_thread.join()

    def _log_event(self, notification: WattsonNotification):
        if notification.notification_topic == WattsonNetworkNotificationTopic.LINK_PROPERTY_CHANGED:
            data = notification.notification_data
            event = {
                "type": notification.notification_topic,
                "timestamp": data.get("received_ts"),
                "link": data.get("link"),
                "property_name": data.get("property_name"),
                "property_value": data.get("property_value"),
                "received_ts": data.get("received_ts")
            }
            self._events.append(event)
        if notification.notification_topic == "wattson-performance-server-marker":
            data = notification.notification_data
            self._events.append({
                "type": notification.notification_topic,
                "timestamp": data.get("timestamp"),
                "description": data.get("description")
            })

    def _measurement_runner(self):
        self.logger.info("Creating socket")
        server_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        server_socket.bind((self._ip, self._port))
        server_socket.setblocking(False)
        self.logger.info(f"Bound to {self._ip}:{self._port}")
        self.logger.info(f"Using buffer size of {self._buffer_size} bytes")
        self.logger.info(f"Using payload size of {self._payload_size} bytes")

        _last_packet_timestamp_ns = 0
        _loss_trigger_ns = 2 * 1000**3
        _connected = False

        self._start_timestamp_ns = time.time_ns()

        self._probes = {}
        while not self._terminate_requested.is_set():
            ready = select.select([server_socket], [], [], 1)
            ts_local = time.time_ns()
            time_since_last_packet_ns = ts_local - _last_packet_timestamp_ns
            if not ready[0]:
                if time_since_last_packet_ns > _loss_trigger_ns:
                    if _connected:
                        self.logger.info("Connection lost")
                    _connected = False
                continue
            json_string = server_socket.recv(self._buffer_size).decode("utf-8")
            data = json.loads(json_string)
            _last_packet_timestamp_ns = ts_local

            if not _connected:
                self.logger.info("Connection reestablished")

            _connected = True
            ts_remote = data["timestamp"]
            seq_remote = data["seq"]
            delay = ts_local - ts_remote
            self._probes[seq_remote] = {
                "seq": seq_remote,
                "sent_ts_ns": ts_remote,
                "receive_ts_ns": ts_local,
                "delay_ns": delay,
                "delay_ms": delay / 1000**2,
                "data": data.get("data", False)
            }

    def stop(self):
        self.logger.info("Stopping measurement server...")
        super().stop()
        self._terminate_requested.set()
        if self._measurement_thread is not None:
            self._measurement_thread.join()
        out_file = self._work_dir.joinpath(self._result_file_name)
        self.logger.info(f"Writing results to {str(out_file)}")
        end_ts_ns = time.time_ns()
        duration_ms = (end_ts_ns - self._start_timestamp_ns) / 1000**2
        expected_max_seq = 0
        if self._expected_interval_ms > 0:
            expected_max_seq = int(duration_ms / self._expected_interval_ms)
        results = {
            "start_timestamp_ns": self._start_timestamp_ns,
            "stop_timestamp_ns": end_ts_ns,
            "expected_interval_ms": self._expected_interval_ms,
            "expected_max_seq": expected_max_seq,
            "probes": self._probes,
            "events": self._events,
        }

        with out_file.open("w") as f:
            json.dump(results, f)
        self.logger.info("Stopped measurement server...")
