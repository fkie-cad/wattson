import copy
from pathlib import Path

from wattson.cosimulation.control.scenario_extension import ScenarioExtension
from wattson.cosimulation.simulators.network.components.wattson_network_docker_host import WattsonNetworkDockerHost
from wattson.cosimulation.simulators.network.components.wattson_network_docker_router import WattsonNetworkDockerRouter


class ReplaceWithDockerHosts(ScenarioExtension):
    def extend_pre_physical(self):
        apply_to_roles = self.config.get("roles", True)
        exclude_roles = self.config.get("exclude_roles", ["sim-control"])
        custom_images = self.config.get("custom_images", {})
        include_nodes = self.config.get("nodes", [])

        if "sim-control" not in exclude_roles:
            exclude_roles.append("sim-control")

        logger = self.co_simulation_controller.logger.getChild("DockerHosts")
        network_emulator = self.co_simulation_controller.network_emulator
        model_manager = self.co_simulation_controller.get_model_manager()
        facilities = model_manager.get_models("facility")

        logger.info(f"{repr(custom_images)}")

        hosts_to_replace = []
        for host in network_emulator.get_hosts():
            # logger.info(f"{host.entity_id}: {host.get_roles()}")
            if isinstance(exclude_roles, list):
                if len(set(exclude_roles).intersection(host.get_roles())) > 0:
                    continue

            if isinstance(host, WattsonNetworkDockerHost):
                # Only replace image when it is part of the custom_images dict
                if host.entity_id not in custom_images:
                    continue

            if apply_to_roles is True:
                hosts_to_replace.append(host)
                continue
            
            if isinstance(apply_to_roles, list):
                if len(set(apply_to_roles).intersection(host.get_roles())) > 0:
                    hosts_to_replace.append(host)
                    continue
                
            if isinstance(include_nodes, list) and host.entity_id in include_nodes:
                hosts_to_replace.append(host)
                continue

        for host in hosts_to_replace:
            config = host.get_config()
            docker_config = copy.deepcopy(config)

            image = "wattson-base"
            docker_class = WattsonNetworkDockerHost

            if host.has_role("rtu"):
                image = "wattson-rtu-ssh"
            elif host.has_role("mtu") or host.has_role("ccx"):
                image = "wattson-mtu-ssh"
            elif host.has_role("router"):
                image = "wattson-router"
                if host.has_role("attacker"):
                    image = "wattson-router-attacker"
                docker_class = WattsonNetworkDockerRouter
            elif host.has_role("attacker"):
                image = "wattson-attacker"
            image = custom_images.get(host.entity_id, image)
            docker_config["image"] = image
            docker_config["type"] = "docker-host"

            logger.info(f"Replacing host {host.entity_id} with docker image {image}")

            docker_host = docker_class(id=docker_config["id"], config=docker_config)
            network_emulator.replace_node(host, docker_host)
            # Add log as volume
            host_folder = docker_host.get_host_root(False)
            host_log_folder = host_folder.joinpath("log")
            try:
                host_log_folder.mkdir(parents=True, exist_ok=True)
            except Exception:
                logger.warning(f"Could not create log folder for {docker_host.entity_id}")
            log_folder = Path("/var/log")
            docker_host.add_volume("log", str(host_log_folder.absolute()), str(log_folder))
