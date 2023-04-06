from threading import Event
from typing import Type, Optional
import logging

from wattson.analysis.statistics.client.statistic_client import StatisticClient
from wattson.datapoints.manager import DataPointManager
from wattson.hosts.rtu.rtu_iec104 import RtuIec104
from wattson.hosts.rtu.rtu_logic import RTULogic

from wattson.iec104.common import SERVER_UPDATE_PERIOD_MS
from wattson.powergrid import CoordinationClient

from wattson.util import apply_args_from_kwargs, get_logger

from wattson.iec104.interface.server import IECServerInterface


class RTU:
    """
        Model of an RTU. The naming of the data points was adopted from the
        cps-ids package. The "server_datapoints" are those which are used for the
        "up-stream" communication, hence the communication to the central
        SCADA entity on the WAN.
        Implemented features of the entire RTU:
         - set/set values of datapoints
         - send periodic updates
         - receive ASDUs and log them
         - respond to general interrogation
    """

    def __init__(
        self,
        iec_server_class: Type[IECServerInterface],
        server_datapoints: list,
        **kwargs,
    ):
        self.ip = ""
        self.hostname = ""
        self.coa = 0
        self.iec_server_class = iec_server_class
        self.data_point_list = server_datapoints
        self.data_point_dict = {p["identifier"]: p for p in server_datapoints}
        self._stop_event = Event()

        self.power_net = kwargs.get("power_net")

        # print(f' Kwargs: {kwargs}' )

        apply_args_from_kwargs(self, ["coa", "ip", "hostname"], **kwargs)
        self.periodic_update_ms = kwargs.get(
            "periodic_update_ms", SERVER_UPDATE_PERIOD_MS
        )
        self.periodic_update_start = kwargs.get("periodic_update_start", 0)
        self.periodic_updates_enable = kwargs.get("periodic_updates_enable", True)
        self.iec104_port = kwargs.get("iec104_port", 2404)

        self.logger = kwargs.get("logger")
        if self.logger is None:
            self.logger = get_logger(
                str(self.coa),
                f"Wattson.RTU {self.coa}",
                level=logging.INFO,
                use_context_logger=False,
                use_async_logger=False
            )
        self.logger.info(f"Starting RTU {self.coa}")

        self.statistics_config = kwargs.get("statistics", {})

        self.statistics = StatisticClient(
            ip=self.statistics_config.get("server"),
            host=self.hostname,
            logger=self.logger,
        )
        self.statistics.start()
        self.statistics.log("start")

        coord_ip = kwargs.get("coord_ip", None)
        self.coord_client = None
        if coord_ip is not None:
            self.coord_client = CoordinationClient(
                coord_ip, node_id=f"RTU.{self.coa}.MAIN"
            )

        self.fields = kwargs.get("fields", None)

        logics = kwargs.get("logics", [])
        self.logics = []
        for logic_info in logics:
            logic_cls = logic_info["class"]
            logic_arguments = logic_info["config"]
            logic: RTULogic = logic_cls(self, **logic_arguments)
            logic.configure()
            self.logics.append(logic)

        # Identify protocols and create Sockets (IEC104, MODBUS/TCP, ...)
        self._initialize_protocol_sockets()

        self.manager = DataPointManager(
            str(self.ip),
            self.data_point_dict,
            {
                "modbus": {
                    "field_devices": self.fields,
                    "host": str(self.hostname),
                    "cache_decay": 5,
                    "statistics": self.statistics,
                    "statistics_config": self.statistics_config,
                },
                "pandapower": {
                    "host": str(self.hostname),
                    "coordinator_ip": coord_ip,
                    "cache_decay": 5,
                    "statistics": self.statistics,
                    # "statistics_config": self.statistics_config,
                },
                "register": {"host": str(self.hostname)},
                "copy": {"host": str(self.hostname)},
            },
            logger=self.logger,
        )

        # self.logger.info(f"{self.data_point_dict}")

    def __str__(self):
        return f"RTU {self.hostname}"

    def start(self):
        if self.coord_client is not None:
            self.coord_client.start()
        self.logger.info(f"Starting {self.__str__()}")
        self.manager.start()
        self.logger.info("RTU Data Point Manager ready, starting Protocol Sockets")
        self.start_sockets()
        for logic in self.logics:
            logic.on_start()

    def start_sockets(self):
        self.logger.info("Starting protocol sockets")
        for _, socket in self.protocol_sockets.items():
            socket.start()

    def stop(self):
        self.logger.info(f"Stopping {self.__str__()}:")
        self.logger.info(f"  Logics...")
        for logic in self.logics:
            logic.on_stop()
        self.logger.info(f"  Manager...")
        self.manager.stop()
        self.logger.info(f"  Sockets...")
        self.stop_sockets()
        self.logger.info(f"  Statistics...")
        self.statistics.stop()
        self.logger.info(f"  Done")

    def stop_sockets(self):
        for sock_type, socket in self.protocol_sockets.items():
            self.logger.info(f"      Stopping socket: {sock_type}")
            socket.stop()

    def get_104_socket(self) -> Optional[IECServerInterface]:
        if "60870-5-104" in self.protocol_sockets:
            return self.protocol_sockets["60870-5-104"].server
        return None

    def _initialize_protocol_sockets(self):
        # Identify protocols
        self.protocol_sockets = {}
        for identifier, dp in self.data_point_dict.items():
            protocol = dp["protocol"]
            if protocol not in self.protocol_sockets:
                if protocol == "60870-5-104":
                    self._init_iec104_socket()
                    # SHOULD BE MOVED
                    # self._init_modbus_backend()
                elif protocol == "MODBUS/TCP":
                    # self._init_modbus_backend()
                    raise NotImplementedError("No MODBUS/TCP Handler implemented")
                elif protocol == "61850":
                    raise NotImplementedError("No 61850 Handler implemented")
                else:
                    raise NotImplementedError(f"No Handler for {protocol} implemented")


    # IEC 104 specific socket
    def _init_iec104_socket(self):
        rtu_iec104 = RtuIec104(
            self,
            periodic_update_ms=self.periodic_update_ms,
            periodic_update_start=self.periodic_update_start,
            periodic_updates_enable=self.periodic_updates_enable,
            port=self.iec104_port
        )
        rtu_iec104.setup_socket()
        self.protocol_sockets["60870-5-104"] = rtu_iec104

    # def _init_modbus_backend(self):
    #     self.protocol_sockets["MODBUS/TCP"] = MODBUS_Client_Maintainer(
    #         self.fields, statistics=self.statistics
    #     )

    def wait(self):
        self.logger.info("Waiting for Stop Event")
        self._stop_event.wait()
        self.logger.info("Got Stop Event")
