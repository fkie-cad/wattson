import signal
from subprocess import PIPE
from time import sleep
from typing import TYPE_CHECKING, Optional, Callable

from wattson.cosimulation.simulators.network.components.wattson_network_node import WattsonNetworkNode
from wattson.services.configuration import ServiceConfiguration
from wattson.services.wattson_service import WattsonService
from wattson.util import get_logger

if TYPE_CHECKING:
    from wattson.services.wattson_service_interface import WattsonServiceInterface


class WattsonIDSService(WattsonService):

    def __init__(self, service_configuration: 'ServiceConfiguration', network_node: 'WattsonNetworkNode'):
        super().__init__(service_configuration, network_node)
        self.logger = get_logger("IDS", "IDS")
        self.config = self._service_configuration.copy()
        ip_str = "127.0.0.1" if "ip" not in service_configuration else service_configuration["ip"]
        del self.config["priority"]
        if "elasticsearch_host" not in service_configuration:
            self.config["elasticsearch_host"] = ip_str
        if "kibana_host" not in service_configuration:
            self.config["kibana_host"] = ip_str
        if "kibana_port" not in service_configuration:
            self.config["kibana_port"] = "5601"
        if "elasticsearch_port" not in service_configuration:
            self.config["elasticsearch_port"] = "9200"
        if "filebeat_inputs" not in service_configuration:
            self.config["filebeat_inputs"] = {"zeek-conn-log": ["/opt/zeek/logs/current/conn.log"],
                                              "suricata-eve-log": ["/var/log/suricata/eve.json"]}
        if "nameservers" in service_configuration:
            self.config["nameservers"] = service_configuration["nameservers"].copy()
        self.config["suricata_interfaces"] = []
        for intf in service_configuration["suricata_interfaces"]:
            name = f"{self.network_node.system_name}-{intf.system_name}"
            self.config["suricata_interfaces"].append(name)
        service_configuration["suricata_interfaces"] = self.config["suricata_interfaces"].copy()

        self.config["zeek_worker"] = {}
        for i, intf in enumerate(service_configuration["zeek_worker"]):
            self.config["zeek_worker"][f"worker-{i + 1}"] = f"{self.network_node.system_name}-{intf.system_name}"
        service_configuration["zeek_worker"] = self.config["zeek_worker"].copy()

        self.config["user"] = service_configuration.get("user", "elastic")
        self.config["password"] = service_configuration.get("password", "wattson")

        self.kibana_process = None
        self.filebeat_process = None
        self.suricata_process = None

        self.config["host_name"] = self.network_node.system_name

    def start(self, refresh_config: bool = False):
        super().start(refresh_config=refresh_config)
        self.network_node.popen(["python3", "/wattson/build/edit_config_files.py", f'"{str(self.config)}"'],
                                stdout=PIPE)
        self.network_node.popen(["service", "elasticsearch", "start"], stdout=PIPE)
        self.kibana_process = self.network_node.popen(["/usr/share/kibana/bin/kibana", "--allow-root"], stdout=PIPE)
        sleep(30)
        self.filebeat_process = self.network_node.popen(
            ["/usr/share/filebeat/bin/filebeat", "run", "-c", "/etc/filebeat/filebeat.yml"], stdout=PIPE)
        # 400 Bad Request Errors:
        # see https://github.com/elastic/elastic-agent-libs/issues/181
        self.network_node.popen(["/opt/zeek/bin/zeekctl", "deploy"], stdout=PIPE)
        self.network_node.popen(["/opt/zeek/bin/zeekctl", "start"], stdout=PIPE)
        self.suricata_process = self.network_node.popen(
            ["/usr/bin/suricata", "-c", "/etc/suricata/suricata.yaml", "-D", "--af-packet"], stdout=PIPE)
        self.network_node.popen(["python3", "/wattson/build/import_dashboards.py", f'"{str(self.config)}"'],
                                stdout=PIPE)
        return True

    def stop(self, wait_seconds: float = 5, auto_kill: bool = False,
             async_callback: Optional[Callable[['WattsonServiceInterface'], None]] = None) -> bool:
        success = super().stop(wait_seconds=wait_seconds, auto_kill=auto_kill, async_callback=async_callback)
        self.network_node.exec(["service", "elasticsearch", "stop"])
        self.stop_wait_and_kill_process(self.kibana_process)
        self.stop_wait_and_kill_process(self.suricata_process)
        self.network_node.popen(["/opt/zeek/bin/zeekctl", "stop"])
        self.stop_wait_and_kill_process(self.filebeat_process)
        return success

    def stop_wait_and_kill_process(self, process):
        if process is None:
            return
        process.send_signal(signal.SIGTERM)
        if process.wait(5) is None:
            process.kill()

    def get_stderr(self):
        return self.get_stdout()

    def get_stdout(self):
        return self.get_log_handle()
