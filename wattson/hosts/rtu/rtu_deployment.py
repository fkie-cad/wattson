import json
import sys
from pathlib import Path
from typing import Dict
import time

import pandas

import wattson
from wattson.deployment import PythonDeployment
from wattson.hosts.rtu.logic.test_logic import TestLogic
from wattson.hosts.rtu.rtu import RTU
# from wattson.hosts.rtu_modbus.modbus_client import MODBUS_Ccleint
from wattson.util.misc import dynamic_load_class_from_file, dynamic_load_class
from wattson.hosts.rtu.logic.spontaneous_logic import SpontaneousLogic

class RtuDeployment(PythonDeployment):
    def __init__(self, configuration: Dict):
        super().__init__(configuration)
        self.config = configuration
        if "server_class" in self.config:
            self.iec_server_class = self.config["server_class"]
        else:
            #from wattson.iec104.pyiec104.server.IEC104Slave import IEC104Slave
            from wattson.iec104.implementations.c104.server import IEC104Server
            self.iec_server_class = IEC104Server
            # self.modbus_server_class = MODBUS_CLIENT

        self.net = None
        if "powernet" in configuration:
            self.net = self.load_powernet("powernet")
        self.coordinator_ip = self.config["coordinator_mgm"]
        self.nodeid = self.config["nodeid"]
        self.coa = self.config["coa"]
        self.ip_address = self.config["ip"]
        self.datapoints = self.config["datapoints"]
        self.periodic_update_ms = int(self.config["periodic_update_ms"])
        self.do_periodic_updates = self.config.get("do_periodic_updates", True)
        self.fields = self.config["fields"]
        self.scenario_path = Path(self.config["scenario_path"])
        # TODO SET BACK IF ELSE in seconds
        self.periodic_update_start_at = self.config.get("periodic_update_start", {}).get(self.nodeid, 0)
        self.statistics = self.config.get("statistics", {})
        self.rtu_logics = []
        self.rtu = None
        rtu_logics = self.config["rtu_logic"].get(self.nodeid, None)

        self.rtu_logic_kwargs = {}

        if rtu_logics is None:
            rtu_logics = self.config["rtu_logic"].get("*", None)
        if rtu_logics is not None:
            for logic in rtu_logics:
                l_args = {}
                if type(logic) == str:
                    o_cls = dynamic_load_class(logic)
                elif type(logic) == dict:
                    logic_class = logic.pop("class")
                    o_cls = dynamic_load_class(logic_class)
                    l_args = logic
                elif type(logic) == list:
                    file = self.scenario_path.joinpath(logic[0])
                    cls = logic[1]
                    o_cls = dynamic_load_class_from_file(file, cls)
                else:
                    self._deployment_logger.warning("Invalid Logic Configuration! Skipping...")
                    continue
                self.rtu_logics.append({"class": o_cls, "config": l_args})
        return

    def start(self):
        self.rtu = RTU(
            self.iec_server_class,
            self.datapoints,
            coa=int(self.coa),
            ip=self.ip_address,
            coord_ip=self.coordinator_ip,
            hostname=self.nodeid,
            fields=self.fields,
            periodic_update_ms=self.periodic_update_ms,
            periodic_updates_enable=self.do_periodic_updates,
            periodic_update_start=self.periodic_update_start_at,
            logics=self.rtu_logics,
            statistics=self.statistics,
            power_net=self.net
        )
        self.rtu.start()
        print("Waiting for RTU to terminate")
        self.rtu.wait()
        print("RTU terminated")
        return

    def stop(self):
        self.rtu.stop()
        return
