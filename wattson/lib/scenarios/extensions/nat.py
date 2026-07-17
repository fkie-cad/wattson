import ipaddress

from wattson.cosimulation.control.scenario_extension import ScenarioExtension
from wattson.cosimulation.exceptions import NetworkNodeNotFoundException
from wattson.cosimulation.simulators.network.components.wattson_network_nat import WattsonNetworkNAT


class NAT(ScenarioExtension):
    def extend_pre_physical(self):
        nat_switch_name = self.config.get("nat_switch_name", "Office-sw-external")
        nat_switch_name_alternative = self.config.get("nat_switch_name_alternative", "Control Center-sw-edge")
        allowed_subnets = self.config.get("allowed_subnets", ["172.16.0.0/16"])

        network_emulator = self.co_simulation_controller.network_emulator

        if len(network_emulator.find_nodes_by_role("nat")) > 0:
            self.co_simulation_controller.logger.info("[NAT] Network Emulator already has a NAT host - skipping creation")
            return

        try:
            nat_switch = self.co_simulation_controller.network_emulator.find_node_by_name(nat_switch_name)
        except NetworkNodeNotFoundException as e:
            try:
                nat_switch = self.co_simulation_controller.network_emulator.find_node_by_name(nat_switch_name_alternative)
            except NetworkNodeNotFoundException as e:
                self.co_simulation_controller.logger.error(f"[NAT] Could not find NAT switch: {e=}")
                return


        nat_ip = network_emulator.get_unused_ip(nat_switch.get_subnets(include_management=False)[0])

        # Add NAT
        self.co_simulation_controller.logger.info(f"[NAT] Adding NAT to {nat_switch.display_name} with IP {nat_ip}")
        nat = WattsonNetworkNAT(id="nat")
        network_emulator.add_host(nat)
        network_emulator.connect_nodes(nat, nat_switch, interface_a_options={"ip": nat_ip, "prefix_length": 24})
        for allowed_subnet in allowed_subnets:
            nat.allow_traffic_from_subnet(ipaddress.IPv4Network(allowed_subnet))
