import shlex
import subprocess
from pathlib import Path
from typing import Optional, Callable

import yaml

from wattson.services.deployment import PythonDeployment
from wattson.services.wattson_service import WattsonService
from wattson.util import get_logger

PACKETBEAT_CONFIG = """
output.elasticsearch:
    hosts: ["0.0.0.0:9200"]
    
setup.kibana:
    host: "0.0.0.0:5601"
setup.dashboards.enabled: true

packetbeat.interfaces.type: af_packet
packetbeat.interfaces.device: any

packetbeat.flows:
  timeout: 30s
  period: 10s
  enabled: true


packetbeat.protocols:
- type: dhcpv4
  ports: [67, 68]

- type: dns
  ports: [53]

- type: http
  ports: [80, 8080, 8081, 5000, 8002]

- type: memcache
  ports: [11211]

- type: mysql
  ports: [3306,3307]

- type: pgsql
  ports: [5432]

- type: redis
  ports: [6379]

- type: thrift
  ports: [9090]

- type: mongodb
  ports: [27017]

- type: cassandra
  ports: [9042]

- type: tls
  ports: [443, 993, 995, 5223, 8443, 8883, 9243]

#- type: iec104
#  ports: [2404]
"""


class WattsonPacketbeatService(WattsonService):

    def __init__(self, service_configuration, network_node):
        super().__init__(service_configuration, network_node)
        self.process_id = None
        self.logger = get_logger("WattsonLocalPacketbeatService", "WattsonLocalPacketbeatService")
        self.elk_ip = self._service_configuration.get("elk_ip")
        self.interface = self._service_configuration.get("interface", "any")
        self.working_dir = self._service_configuration.get("working_dir")

    def start(self, refresh_config: bool = False):
        super().start(refresh_config=refresh_config)
        yaml_file = Path(self.working_dir) / f"packetbeat.yml"
        kibana_port = self._service_configuration.get("kibana_port", 5601)
        elastic_port = self._service_configuration.get("elastic_port", 9200)
        if self.elk_ip is not None:
            self.logger.info("Updating packetbeat config with ELK IP")
            config = yaml.load(PACKETBEAT_CONFIG, Loader=yaml.Loader)
            config["setup.kibana"]["host"] = f"{self.elk_ip}:{kibana_port}"
            config["output.elasticsearch"]["hosts"] = [f"{self.elk_ip}:{elastic_port}"]
            config["packetbeat.interfaces.device"] = self.interface
            config_content = yaml.dump(config)
            # Name of shipper must be set at the !end! of the yml file
            config_content += f"\nname: {self.network_node.generate_display_name()}\n"
            escaped_config = shlex.quote(config_content)
            self.network_node.exec(f"echo {escaped_config} > {yaml_file}", shell=True, use_shlex=False)
        p = self.network_node.popen(f"packetbeat -e -c {yaml_file} --path.config {self.working_dir} --path.data {self.working_dir} --path.logs {self.working_dir}")
        self.logger.info("Started Packetbeat")
        self.process_id = p.pid
        self._process = p
        return p.poll() is None

    def stop(self, wait_seconds: float = 5, auto_kill: bool = False, async_callback: Optional[Callable[['WattsonServiceInterface'], None]] = None) -> bool:
        success = super().stop(wait_seconds=wait_seconds, auto_kill=auto_kill, async_callback=async_callback)
        if self.process_id:
            self.network_node.exec(f"kill {self.process_id}")
            self.logger.info("Stopped Packetbeat.")
        return success
