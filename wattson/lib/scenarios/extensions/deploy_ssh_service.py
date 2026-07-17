import copy
import fnmatch
from pathlib import Path
from typing import Optional

from wattson.cosimulation.control.scenario_extension import ScenarioExtension
from wattson.cosimulation.simulators.network.components.wattson_network_docker_host import WattsonNetworkDockerHost
from wattson.cosimulation.simulators.network.components.wattson_network_docker_router import WattsonNetworkDockerRouter
from wattson.services.configuration import ServiceConfiguration
from wattson.services.management.wattson_ssh_service import WattsonSshService
from wattson.util.services.ssh import generate_ssh_user_and_password


class DeploySshService(ScenarioExtension):
    def extend_pre_physical(self):
        apply_to_roles = self.config.get("roles", True)
        exclude_roles = self.config.get("exclude_roles", ["sim-control"])
        apply_to_nodes = self.config.get("nodes", True)
        exclude_nodes = self.config.get("exclude_nodes", [])

        default_password = self.config.get("default_password", "admin")
        weak_password = self.config.get("weak_password", "pass1234")

        default_user = self.config.get("default_user", "admin")

        credentials = self.config.get("credentials", "auto")

        if "sim-control" not in exclude_roles:
            exclude_roles.append("sim-control")

        logger = self.co_simulation_controller.logger.getChild("DeploySshService")
        network_emulator = self.co_simulation_controller.network_emulator

        def get_keyword_credentials(_keyword: str, _host, _keyword_as_password: bool = False, _default_user_generated: bool = False) -> dict:
            _credentials = {}
            generated_user, generated_password = generate_ssh_user_and_password(
                co_simulation_controller=self.co_simulation_controller,
                node=_host,
                password_prefix="",
                password_postfix="!"
            )

            if _keyword == "auto":
                _credentials[generated_user] = generated_password
            elif _keyword == "auto_default":
                _credentials[default_user] = generated_password
            elif _keyword == "default":
                _credentials[default_user] = default_password
            elif _keyword == "weak":
                _credentials[default_user] = weak_password
            elif _keyword_as_password:
                if _default_user_generated:
                    _credentials[generated_user] = _keyword
                else:
                    _credentials[default_user] = _keyword_as_password

            return _credentials

        def get_credential_dict(_host, _credential_info=None) -> dict:
            _credentials = {}
            if _credential_info is None:
                _credential_info = credentials
            if isinstance(_credential_info, str):
                # All nodes should get auto generated credentials
                return get_keyword_credentials(_credential_info, _host, _keyword_as_password=True)
            if isinstance(_credential_info, list):
                # All nodes should get the same set of credentials
                for credential in _credential_info:
                    if isinstance(credential, str):
                        _credentials.update(get_keyword_credentials(credential, _host))
                    elif isinstance(credential, dict):
                        for key, value in credential.items():
                            _credentials.update({key: value})
            if isinstance(_credential_info, dict):
                # Credentials per node / wildcard matching
                node_credentials = _credential_info.get(_host.entity_id)
                if node_credentials is None:
                    for key in sorted(_credential_info.keys(), key=lambda k: len(k), reverse=True):
                        if fnmatch.fnmatch(_host.entity_id, key):
                            return get_credential_dict(_host, _credential_info.get(key))
                    return _credentials
                if isinstance(node_credentials, dict):
                    return node_credentials
                return _credentials
                # return get_credential_dict(_host, node_credentials)
            return _credentials

        for host in network_emulator.get_hosts():
            if not isinstance(host, WattsonNetworkDockerHost):
                continue
            if isinstance(apply_to_roles, list):
                if len(set(apply_to_roles).intersection(host.get_roles())) == 0:
                    continue
            if len(set(exclude_roles).intersection(host.get_roles())) > 0:
                continue
            if isinstance(apply_to_nodes, list):
                if host.entity_id not in apply_to_nodes:
                    continue
            if host.entity_id in exclude_nodes:
                continue

            host_credentials = get_credential_dict(host)
            if len(host_credentials) == 0:
                logger.warning(f"No credentials found for host {host.entity_id}")
            logger.info(f"Adding SSH Service to {host.entity_id}")
            if not isinstance(host_credentials, dict):
                logger.warning(f"Not a dict - {repr(host_credentials)}")
                # continue
            for username, password in host_credentials.items():
                logger.info(f"  Adding {username}:{password}")
            service_configuration = ServiceConfiguration(users=host_credentials)
            host.add_service(WattsonSshService(service_configuration=service_configuration, network_node=host))
