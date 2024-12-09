from pathlib import Path
from typing import Dict

from wattson.services.deployment import PythonDeployment
from wattson.hosts.rtu.rtu import RTU
from wattson.util.misc import dynamic_load_class_from_file, dynamic_load_class


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
        if "power_grid" in configuration:
            self.net = self.load_power_grid("power_grid")

        self.wattson_client_config = self.config.get("wattson_client_config")

        self.nodeid = self.config["nodeid"]
        self.entity_id = self.config["entityid"]
        self.coa = self.config["coa"]
        self.ip_address = self.config["ip"]
        self.datapoints = self.config["datapoints"]
        self.allowed_mtu_ips = self.config.get("allowed_mtu_ips", True)
        self.periodic_update_ms = int(self.config["periodic_update_ms"])
        self.do_periodic_updates = self.config.get("do_periodic_updates", True)
        self.fields = self.config.get("fields", {})
        self.scenario_path = Path(self.config["scenario_path"])
        self.use_syslog = self.config.get("use_syslog", False)
        # TODO SET BACK IF ELSE in seconds
        self.periodic_update_start_at = self.config.get("periodic_update_start", 0)
        self.local_control = self.config.get("local_control", False)
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
            entity_id=self.entity_id,
            ip=self.ip_address,
            wattson_client_config=self.wattson_client_config,
            hostname=self.nodeid,
            fields=self.fields,
            periodic_update_ms=self.periodic_update_ms,
            periodic_updates_enable=self.do_periodic_updates,
            periodic_update_start=self.periodic_update_start_at,
            logics=self.rtu_logics,
            statistics=self.statistics,
            power_grid=self.net,
            allowed_mtu_ips=self.allowed_mtu_ips,
            use_syslog=self.use_syslog,
            local_control=self.local_control
        )
        self.rtu.start()
        self.rtu.wait()
        print("RTU terminated")
        return

    def stop(self):
        self.rtu.stop()
        return
