import threading
from typing import Union, Optional, Type, Any, List

try:
    import mininet.nodelib  # without this import tings break
    import mininet.net
    from mininet.cli import CLI
    import mininet.log
    import mininet.node
except ImportError:
    raise ImportError("Mininet is not installed")

import wattson.util
from wattson.cosimulation.exceptions import NamespaceNotFoundException, InvalidSimulationControlQueryException, \
    NetworkException
from wattson.cosimulation.control.messages.wattson_query import WattsonQuery
from wattson.cosimulation.control.messages.wattson_response import WattsonResponse
from wattson.cosimulation.simulators.network.components.network_link_model import NetworkLinkModel
from wattson.cosimulation.simulators.network.components.wattson_network_docker_host import WattsonNetworkDockerHost
from wattson.cosimulation.simulators.network.components.wattson_network_entity import WattsonNetworkEntity
from wattson.cosimulation.simulators.network.components.wattson_network_host import WattsonNetworkHost
from wattson.cosimulation.simulators.network.components.wattson_network_interface import WattsonNetworkInterface
from wattson.cosimulation.simulators.network.components.wattson_network_link import WattsonNetworkLink
from wattson.cosimulation.simulators.network.components.wattson_network_nat import WattsonNetworkNAT
from wattson.cosimulation.simulators.network.components.wattson_network_node import WattsonNetworkNode
from wattson.cosimulation.simulators.network.components.wattson_network_router import WattsonNetworkRouter
from wattson.cosimulation.simulators.network.components.wattson_network_switch import WattsonNetworkSwitch
from wattson.cosimulation.simulators.network.constants import NETWORK_ENTITY
from wattson.cosimulation.simulators.network.messages.wattson_network_query import WattsonNetworkQuery
from wattson.cosimulation.simulators.network.messages.wattson_network_notificaction_topics import WattsonNetworkNotificationTopic
from wattson.cosimulation.control.messages.wattson_notification import WattsonNotification

from wattson.cosimulation.simulators.network.network_emulator import NetworkEmulator
from wattson.networking.namespaces.namespace import Namespace
from wattson.util.progress_printer import ProgressPrinter
from wattson.util.events.wait_event import WaitEvent


class MininetEmulator(NetworkEmulator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._controller_cls = None
        self._mininet = None
        self._nodes = []
        self._links = []
        self._interfaces = []
        self._started_event = threading.Event()
        self._network_namespaces = {}
        self._print_progress: bool = kwargs.get("print_progress", True)
        self.set_mininet_log_level("warning")
        for namespace in Namespace.get_namespaces():
            if namespace.name.startswith("w_"):
                message = "There are already Wattson-related namespaces. " \
                          "Please clean any running Wattson simulation first."
                self.logger.critical(message)
                raise NetworkException(message)

    def set_mininet_log_level(self, log_level: str = "info"):
        mininet.log.setLogLevel(log_level)

    def cli(self):
        CLI(self._mininet)

    def start(self):
        self.logger.info("Starting network emulation")
        self._mininet = mininet.net.Mininet(
            ipBase=self._config["ip_base"],
            link=wattson.util.dynamic_load_class(self._link_cls),
            switch=wattson.util.dynamic_load_class(self._switch_cls),
            build=False,
        )
        self._links = []
        self._interfaces = []

        # Add Controller
        if self._controller_cls is not None:
            controller = wattson.util.dynamic_load_class(self._controller_cls)
            port = self._config["controller_port"]
            self.logger.info(f"  Adding controller {controller.__name__}")
            c1 = controller("WattsonController", port=port)
            self._mininet.addController(c1)

        self.logger.info("  Organizing entities")
        progress_printer = ProgressPrinter(max_progress=len(self.get_graph().nodes), enable_print=self._print_progress)
        progress_printer.start()
        for node_id, node_data in self.get_graph().nodes.items():
            node = node_data[NETWORK_ENTITY]
            if isinstance(node, WattsonNetworkNode):
                self._nodes.append(node)
            elif isinstance(node, WattsonNetworkInterface):
                self._interfaces.append(node)
            elif isinstance(node, WattsonNetworkLink):
                self._links.append(node)
            else:
                self.logger.critical(f"Unknown node of class {node.__class__.__name__} found")
            progress_printer.inc()
        progress_printer.stop()

        self.logger.info("  Adding nodes")
        progress_printer = ProgressPrinter(max_progress=len(self._nodes), enable_print=self._print_progress)
        progress_printer.start()
        for node in self._nodes:
            self._add_mininet_entity(node)
            progress_printer.inc()
        progress_printer.stop()

        self.logger.info("  Adding links and interfaces")

        progress_printer = ProgressPrinter(max_progress=len(self._links), enable_print=self._print_progress, on_stop_margin=True)
        progress_printer.start()
        for link in self._links:
            self._add_mininet_entity(link)
            progress_printer.inc()
        progress_printer.stop()

        self.logger.info("  Starting mininet")
        self._mininet.start()

        if self._controller_cls is not None:
            while not self._mininet.waitConnected(timeout=10):
                self.logger.info("  Waiting for switches to connect")

        self.logger.info("  Fixing router default routes")
        for router in self.get_routers():
            self._fix_router_loopback(router)

        # Start Wattson WattsonNetworkEntity instances
        for entity_node in self.get_entities():
            entity_node.start()
            if isinstance(entity_node, WattsonNetworkNode):
                self.get_namespace(entity_node, raise_exception=False)
        self._started_event.set()

    def stop(self):
        self._started_event.clear()
        self.stop_services()
        if self._mininet is not None:
            self.logger.info(f"Stopping Mininet")
            self._mininet.stop()

        self.logger.info(f"Stopping (remaining) entities")
        for entity in self.get_entities():
            entity.stop()

        self.logger.info(f"Cleaning up namespaces")
        for namespace in Namespace.get_namespaces():
            if namespace.name.startswith("w_"):
                namespace.clean()

    @property
    def is_running(self) -> bool:
        return self._started_event.is_set()

    def _add_mininet_entity(self, entity: WattsonNetworkEntity):
        if isinstance(entity, WattsonNetworkNode):
            if isinstance(entity, WattsonNetworkSwitch):
                if self.is_running:
                    self.logger.info(f"Adding Switch: {entity.node_id} ({entity.system_id})")
                entity.emulation_instance = self._mininet.addSwitch(entity.system_id, **entity.get_emulation_entity_config())
                """
                elif isinstance(entity, WattsonNetworkRouter):
                    if self.is_running:
                        self.logger.info(f"Adding Router: {entity.node_id} ({entity.system_id})")
                    if isinstance(entity, WattsonNetworkNativeRouter):
                        # Let Wattson handle the routing
                        entity.emulation_instance = self._mininet.addHost(entity.system_id)
                    else:
                        # Let IPMininet handle the routing
                        entity.emulation_instance = self._mininet.addRouter(entity.system_id)
                """
            elif isinstance(entity, WattsonNetworkDockerHost):
                if self.is_running:
                    self.logger.info(f"Adding Docker Host: {entity.node_id} ({entity.system_id})")
                image = entity.get_full_image()
                volumes_dict_list = entity.get_volumes()
                volumes = []
                for volume in volumes_dict_list:
                    host_path = volume["host_path"]
                    docker_path = volume["docker_path"]
                    permission = volume.get("permission", "rw")
                    if ":" in host_path:
                        self.logger.error(f"Colon found in volume's host path: {host_path}")
                        continue
                    if ":" in docker_path:
                        self.logger.error(f"Colon found in volume's docker path: {docker_path}")
                        continue
                    volumes.append(f"{host_path}:{docker_path}:{permission}")
                command = entity.get_boot_command()
                memory_limit = entity.get_config().get("memory_limit")
                entity.emulation_instance = self._mininet.addHost(
                    entity.system_id,
                    ip="",
                    cls=mininet.node.Docker,
                    dimage=image,
                    volumes=volumes,
                    dcmd=command,
                    mem_limit=memory_limit
                )
            elif isinstance(entity, WattsonNetworkNAT):
                if self.is_running:
                    self.logger.info(f"Adding NAT: {entity.node_id} ({entity.system_id})")
                entity.emulation_instance = self._mininet.addHost(name=entity.system_id, inNamespace=False, ip="")
            elif isinstance(entity, WattsonNetworkHost):
                if self.is_running:
                    self.logger.info(f"Adding Host: {entity.node_id} ({entity.system_id})")
                entity.emulation_instance = self._mininet.addHost(entity.system_id, ip="")

        elif isinstance(entity, WattsonNetworkLink):
            if self.is_running:
                self.logger.info(f"Adding Link: {entity.entity_id} ({entity.system_id})")

            link = entity
            node_a = link.interface_a.node.emulation_instance
            node_b = link.interface_b.node.emulation_instance

            params1 = {}
            params2 = {}
            link_options = {}
            interface: WattsonNetworkInterface
            for i, (interface, params) in enumerate([(link.interface_a, params1), (link.interface_b, params2)], start=1):
                if interface.ip_address is not None:
                    params["ip"] = interface.ip_address_string
                if interface.mac_address is not None:
                    link_options[f"addr{i}"] = interface.mac_address
                link_options[f"intfName{i}"] = interface.interface_name

            link_options.update(self._get_tc_configuration(link.link_model))

            link.add_on_link_property_change_callback(self._on_link_property_changed)
            link.emulation_instance = self._mininet.addLink(
                node1=node_a,
                node2=node_b,
                params1=params1,
                params2=params2,
                **link_options
            )
            link.interface_a.emulation_instance = link.emulation_instance.intf1
            link.interface_b.emulation_instance = link.emulation_instance.intf2
            if self.is_running:
                link.interface_a.start()
                link.interface_b.start()

        elif isinstance(entity, WattsonNetworkInterface):
            # self.logger.warning(f"A WattsonNetworkInterface cannot be added on its own")
            pass

    def reset_all_flows(self):
        """Resets all configures flows of Open vSwitches"""
        self.logger.info("Resetting Flows")
        for switch in self.get_switches():
            switch.reset_flows()

    def add_node(self, node: WattsonNetworkNode):
        super().add_node(node)
        if self.is_running:
            self._add_mininet_entity(node)
            node.start()

    def add_link(self, link: WattsonNetworkLink):
        super().add_link(link)
        if self.is_running:
            self._add_mininet_entity(link)
            link.start()

    def add_interface(self, node: Union[str, WattsonNetworkNode], interface: WattsonNetworkInterface):
        super().add_interface(node, interface)
        if self.is_running:
            self._add_mininet_entity(interface)
            interface.start()

    def deploy_services(self):
        self.logger.info("Starting services")
        services = []
        for node in self.get_nodes():
            if node.has_services():
                for service_id, service in node.get_services().items():
                    if service.autostart:
                        services.append(service)
        progress_printer = ProgressPrinter(max_progress=len(services), on_stop_margin=True, enable_print=self._print_progress)
        progress_printer.start()
        for service in sorted(services, key=lambda s: s.get_priority().get_global(), reverse=True):
            if service.autostart_delay > 0:
                service.delay_start(service.autostart_delay)
            else:
                service.start()
            progress_printer.inc()
        progress_printer.stop()

    def stop_services(self):
        services = []
        for node in self.get_nodes():
            for service_id, service in node.get_services().items():
                services.append(service)

        wait_event = WaitEvent(20)
        progress_printer = ProgressPrinter(len(services), stop_event=wait_event)

        service_status = {}

        def update_service_status(updated_service):
            if updated_service is not None:
                progress_printer.inc()

        self.logger.info("Stopping services")
        progress_printer.start()
        wait_event.start()

        for node in self.get_nodes():
            if node.has_services():
                for service_id, service in node.get_services().items():
                    service_status[service_id] = service
                    node.stop_service(service_id=service_id, auto_kill=True, async_callback=update_service_status)

        if len(service_status) == 0:
            wait_event.complete()

        wait_event.wait()

        not_terminated_services = []
        for service_id, service in service_status.items():
            if service.is_running():
                not_terminated_services.append(service)
        if len(not_terminated_services):
            print(f"{len(not_terminated_services)} service(s) could not be stopped:")
            print(",   ".join([f"{service.id} ({service.name}) @ {service.network_node.display_name}" for service in not_terminated_services]))

        print("", flush=True)

    def get_controllers(self) -> List:
        return self._mininet.controllers

    def get_namespace(self, node: Union[str, WattsonNetworkEntity], raise_exception: bool = True) -> Optional[Namespace]:
        node = self.get_node(node)
        node_id = node.entity_id
        if node_id in self._network_namespaces:
            return self._network_namespaces[node_id]

        if isinstance(node, WattsonNetworkDockerHost):
            return node.get_namespace()

        # Try to create namespace from mininet if not yet created
        try:
            pid = node.emulation_instance.pid
            namespace = Namespace(f"w_{node_id}")
            if not namespace.from_pid(pid=pid):
                self.logger.critical(f"Could not create namespace {namespace.name} from {pid=}")
            self._network_namespaces[node_id] = namespace
            return namespace
        except Exception as e:
            if raise_exception:
                raise NamespaceNotFoundException(f"No network namespace for {node_id}: {e=}")
        return None

    def _fix_router_loopback(self, router: WattsonNetworkRouter):
        cmd = f"ip a s dev lo | grep \"scope global lo\""
        host = router.emulation_instance
        ip_line = host.cmd(cmd)
        ip = None
        for entry in ip_line.split(" "):
            if "/" in entry:
                ip = entry
                break
        if ip is not None:
            self.logger.debug(f"Removing IP: {ip} from router {router.entity_id}")
            host.cmd(f"ip addr del {ip} dev lo")

    def _on_link_property_changed(self, link: WattsonNetworkLink, property_name: str, property_value: Any):
        if link.is_started:
            self._update_tc_properties(link)

    def _update_tc_properties(self, link):
        if not self._started_event.is_set():
            return
        link_model = link.get_link_model()
        tc_options = self._get_tc_configuration(link_model)
        mininet_i_face_a = link.interface_a.emulation_instance
        mininet_i_face_b = link.interface_b.emulation_instance
        mininet_i_face_a.config(**tc_options)
        mininet_i_face_b.config(**tc_options)

    @staticmethod
    def _get_tc_configuration(link_model: NetworkLinkModel) -> dict:
        link_options = {}
        if link_model.bandwidth_bits_per_second is not None:
            link_options["bw"] = link_model.bandwidth_mbps
        if link_model.delay_ms is not None:
            link_options["delay"] = f"{link_model.delay_ms}ms"
        if link_model.jitter_ms is not None:
            link_options["jitter"] = f"{link_model.jitter_ms}ms"
        if link_model.packet_loss_percent is not None:
            link_options["loss"] = link_model.packet_loss_percent
        return link_options

    def handles_simulation_query_type(self, query: Union[WattsonQuery, Type[WattsonQuery]]) -> bool:
        query_type = self.get_simulation_query_type(query)
        return issubclass(query_type, WattsonNetworkQuery) or super().handles_simulation_query_type(query_type)

    def handle_simulation_control_query(self, query: WattsonQuery) -> Optional[WattsonResponse]:
        if not self.handles_simulation_query_type(query):
            raise InvalidSimulationControlQueryException(f"MininetEmulator does not handle {query.__class__.__name__}")
        return super().handle_simulation_control_query(query)

    """
    ####
    #### EMULATION EVENTS
    ####
    """
    def on_entity_start(self, entity: WattsonNetworkEntity):
        super().on_entity_start(entity)

    def on_entity_stop(self, entity: WattsonNetworkEntity):
        super().on_entity_stop(entity)

    def on_entity_remove(self, entity: WattsonNetworkEntity):
        super().on_entity_remove(entity)
        if not self.is_running:
            return
        if isinstance(entity, WattsonNetworkLink):
            self._mininet.delLink(entity.emulation_instance)
        elif isinstance(entity, WattsonNetworkHost):
            self._mininet.delHost(entity.emulation_instance)
        elif isinstance(entity, WattsonNetworkSwitch):
            self._mininet.delSwitch(entity.emulation_instance)
        elif isinstance(entity, WattsonNetworkNode):
            self._mininet.delNode(entity.emulation_instance)

    def on_topology_change(self, trigger_entity: WattsonNetworkEntity, change_name: str = "topology_changed"):
        super().on_topology_change(trigger_entity, change_name)
        if not self.is_running:
            return
        self.send_notification(WattsonNotification(notification_topic=WattsonNetworkNotificationTopic.TOPOLOGY_CHANGED,
                                                   notification_data={"entity_id": trigger_entity.entity_id, "change": change_name}))

    def open_browser(self, node: WattsonNetworkNode) -> bool:
        if node.__class__ == WattsonNetworkHost:
            binary = None
            browsers = ["firefox", "google-chrome", "chromium"]
            for browser in browsers:
                code, lines = node.exec(["which", browser])
                if code == 0 and len(lines) == 1:
                    binary = lines[0]
                    break
            if binary is None:
                return False
            process = node.get_namespace().popen([binary], shell=True)
            node.manage_process(process)
            return process.poll() is None
        return False
