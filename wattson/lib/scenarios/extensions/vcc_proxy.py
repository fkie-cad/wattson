from typing import TYPE_CHECKING

from wattson.cosimulation.control.scenario_extension import ScenarioExtension
from wattson.cosimulation.simulators.network.components.wattson_network_host import WattsonNetworkHost
from wattson.cosimulation.simulators.network.components.wattson_network_switch import WattsonNetworkSwitch

if TYPE_CHECKING:
    from wattson.cosimulation.control.co_simulation_controller import CoSimulationController


class VccProxy(ScenarioExtension):
    def __init__(self, co_simulation_controller: 'CoSimulationController', **kwargs):
        super().__init__(co_simulation_controller, **kwargs)
        # self.proxy_host = None

    def extend_pre_physical(self):
        network_emulator = self.co_simulation_controller.network_emulator
        logger = self.co_simulation_controller.logger.getChild("VccProxy")

        logger.info("Adding VCC proxy interface")
        proxy_host = WattsonNetworkHost(id="proxy")
        # Set as native host
        proxy_host._is_outside_namespace = True
        proxy_host.config.update({"use-default-routes": False})
        network_emulator.add_host(proxy_host)

        ccx_host = network_emulator.find_nodes_by_role("mtu")[0]
        primary_interface = None
        for interface in ccx_host.get_interfaces():
            if interface.is_management:
                continue
            primary_interface = interface
            break
        if primary_interface is None:
            raise RuntimeError("Could not find primary interface for CCX")
        ccx_url = f"http://{primary_interface.ip_address_short_string}"
        subnet = primary_interface.get_subnet(include_management=False)
        target_node = primary_interface.get_next_node()
        if target_node is None:
            raise RuntimeError("Could not find target node for CCX")
        ip_address_proxy = network_emulator.get_unused_ip(subnet)
        if isinstance(target_node, WattsonNetworkSwitch):
            # No IP needed
            switch_interface = target_node.get_spare_interface()
            if switch_interface is not None:
                logger.info(f"Using existing spare interface {switch_interface.interface_name} ({switch_interface.entity_id})")
            proxy_interface, _, _ = network_emulator.connect_nodes(
                proxy_host, target_node,
                interface_a_options={"ip_address": ip_address_proxy, "prefix_length": subnet.prefixlen},
                interface_b=switch_interface
            )
        else:
            node_ip = network_emulator.get_unused_ip(subnet, exclude_ips=[ip_address_proxy])
            proxy_interface, _, _ = network_emulator.connect_nodes(
                proxy_host, target_node,
                interface_a_options={"ip_address": ip_address_proxy, "prefix_length": subnet.prefixlen},
                interface_b_options={"ip_address": node_ip, "prefix_length": subnet.prefixlen}
            )

        proxy_host.config.update({"custom-routes": [{"route": f"{subnet.hostmask}/{subnet.prefixlen}", "interface": proxy_interface.interface_name}]})
        self.co_simulation_controller.queue_post_start_message(f"CCX is available at {ccx_url}")
