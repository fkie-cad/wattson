# Replace a host with a docker host

## Introduction

This document aims to provide a step-by-step guide to extend a Wattson scenario by replacing an existing host with a docker host.

Replace the name for the docker image you want to use. 
For example one from the wattson-docker repo.
Insert the ip of the switch you want to connect your host to.
Check the network.yml of the scenario to find a switch.
Note the "n" before the switch\_id. 
It's important.


```python
import copy

from wattson.cosimulation.control.scenario_extension import ScenarioExtension
from wattson.cosimulation.simulators.network.components.wattson_network_docker_host import WattsonNetworkDockerHost


class DockerRtu(ScenarioExtension):
    def provides_pre_physical(self) -> bool:
        return True
    
    def extend_pre_physical(self):
        rtus_to_replace = self.config.get("rtu_ids", [])
        network_emulator = self.co_simulation_controller.network_emulator
        logger = self.co_simulation_controller.logger.getChild("DockerRTU")
        for rtu_id in rtus_to_replace:
            rtu = network_emulator.get_host(rtu_id)
            if rtu is None:
                logger.error(f"Cannot upgrade non-existing RTU {rtu_id} to a docker host")
                continue
            if not rtu.has_role("rtu"):
                logger.error(f"Host {rtu_id} is not an RTU")
                continue
            rtu_config = rtu.get_config()
            docker_config = copy.deepcopy(rtu_config)
            docker_config["type"] = "docker-host"
            docker_config["image"] = "your-docker-image"
            """
            docker_config["services"] = [{
                "module": "wattson.services.<service-file>",
                "service-type": "python",
                "class": "<service-class-name>",
                "autostart": False,
                "config": {"some-field": "some-value"}
            }]
            """
            docker_rtu = WattsonNetworkDockerHost(id=docker_config["id"], config=docker_config)
            network_emulator.replace_node(rtu, docker_rtu)
            logger.info(f"Upgraded RTU {docker_rtu.entity_id} to a Docker-based RTU")
```