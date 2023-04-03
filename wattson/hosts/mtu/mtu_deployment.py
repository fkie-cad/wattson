import time

from wattson.deployment import PythonDeployment
from wattson.hosts.mtu.mtu import MTU


class MtuDeployment(PythonDeployment):
    def __init__(self, configuration: dict):
        super().__init__(configuration)
        self.config = configuration
        if "client_class" in self.config:
            self.iec_client_class = self.config["master_class"]
        else:
            #from wattson.iec104.implementations.pyiec104.client import IEC104Master
            from wattson.iec104.implementations.c104.client import IEC104Client
            self.iec_client_class = IEC104Client
        self.coordinator_ip = self.config["coordinator_mgm"]
        self.statistics = self.config.get("statistics")
        self.nodeid = self.config["nodeid"]
        self.coa = self.config["coa"]
        self.ip_address = self.config["ip"]
        self.rtus = self.config["rtus"]
        self.datapoints = self.config["datapoints"]
        self.iec104_connect_delay = self.config.get("connect_delay", 0)
        self.do_clock_sync = self.config.get("do_clock_sync", True)
        self.do_general_interrogation = self.config.get("do_general_interrogation", True)
        return

    def start(self):
        self.mtu = MTU(
            self.iec_client_class,
            self.datapoints,
            node_id=self.nodeid,
            coa=self.coa,
            rtus=self.rtus,
            do_general_interrogation=self.do_general_interrogation,
            do_clock_sync=self.do_clock_sync,
            coord_ip=self.coordinator_ip,
            max_rtus=None,
            primary_ip=self.ip_address,
            statistics=self.statistics,
            iec104_connect_delay=self.iec104_connect_delay
        )
        self.mtu.start()
        while True:
            time.sleep(60)

    def stop(self):
        self.mtu.stop()
