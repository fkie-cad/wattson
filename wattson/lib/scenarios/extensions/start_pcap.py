from wattson.cosimulation.control.scenario_extension import ScenarioExtension
from wattson.services.configuration import ServiceConfiguration
from wattson.services.wattson_pcap_service import WattsonPcapService


class StartPcap(ScenarioExtension):
    def extend_pre_physical(self):
        interfaces = self.config.get("interfaces", [])
        network = self.co_simulation_controller.network_emulator
        for interface_name in interfaces:
            node_id, interface_id = interface_name.split(".")
            node = network.get_node(node_id)
            interface = network.get_interface(node_id, interface_id)
            service = WattsonPcapService(interface=interface, service_configuration=ServiceConfiguration(), network_node=node)
            node.add_service(service)
            self.co_simulation_controller.logger.info(f"[PCAP] Starting PCAP on {interface_name} ({interface.interface_name})")
