import multiprocessing.pool
import os
import resource
import subprocess
import threading
import traceback
import typing
from typing import Union, Optional, Dict, Any

import psutil

from wattson.cosimulation.control.messages.wattson_notification import WattsonNotification
from wattson.cosimulation.exceptions import NetworkNodeNotFoundException
from wattson.cosimulation.simulators.network.components.wattson_network_docker_host import WattsonNetworkDockerHost
from wattson.cosimulation.simulators.network.components.wattson_network_entity import WattsonNetworkEntity
from wattson.cosimulation.simulators.network.components.wattson_network_host import WattsonNetworkHost
from wattson.cosimulation.simulators.network.components.wattson_network_interface import WattsonNetworkInterface
from wattson.cosimulation.simulators.network.components.wattson_network_link import WattsonNetworkLink
from wattson.cosimulation.simulators.network.components.wattson_network_node import WattsonNetworkNode
from wattson.cosimulation.simulators.network.components.wattson_network_switch import WattsonNetworkSwitch
from wattson.cosimulation.simulators.network.components.wattson_network_virtual_machine_host import WattsonNetworkVirtualMachineHost
from wattson.cosimulation.simulators.network.emulators.wattson_network_emulator.wrapper.docker_wrapper import DockerWrapper
from wattson.cosimulation.simulators.network.emulators.wattson_network_emulator.wrapper.entity_wrapper import EntityWrapper
from wattson.cosimulation.simulators.network.emulators.wattson_network_emulator.wrapper.interface_wrapper import InterfaceWrapper
from wattson.cosimulation.simulators.network.emulators.wattson_network_emulator.wrapper.link_wrapper import LinkWrapper
from wattson.cosimulation.simulators.network.emulators.wattson_network_emulator.wrapper.native_wrapper import NativeWrapper
from wattson.cosimulation.simulators.network.emulators.wattson_network_emulator.wrapper.ovs_wrapper import OvsWrapper
from wattson.cosimulation.simulators.network.emulators.wattson_network_emulator.wrapper.virtual_machine_wrapper import VirtualMachineWrapper
from wattson.cosimulation.simulators.network.messages.wattson_network_notificaction_topics import WattsonNetworkNotificationTopic
from wattson.cosimulation.simulators.network.network_emulator import NetworkEmulator
from wattson.networking.namespaces.namespace import Namespace
from wattson.util.events.wait_event import WaitEvent
from wattson.util.performance.resettable_timer import ResettableTimer
from wattson.util.progress_printer import ProgressPrinter


class WattsonNetworkEmulator(NetworkEmulator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._wrappers: Dict[str, EntityWrapper] = {}
        self._namespaces: Dict[str, Namespace] = {}
        self._print_progress: bool = kwargs.get("print_progress", True)
        self._async_start: bool = kwargs.get("async_start", True)
        # Size of the ThreadPool to use for the async start. Lower values increase stability at the cost of startup speed
        self._async_threads: int = int(kwargs.get("async_thread", 200))
        self._started = threading.Event()
        # Namespace object to represent the default / initial / system namespace
        self._main_namespace: Namespace = Namespace("w_main")
        self._disable_tc_link = kwargs.get("disable_link_properties", False)

        self._topology_change_timer: Optional[ResettableTimer] = None
        self._topology_change_cache = None
        self._topology_change_lock = threading.Lock()

    def cli(self):
        raise NotImplementedError("WattsonNetworkEmulator does not support a CLI - use the WattsonCLI instead")

    def get_wrapper(self, entity: Union[str, WattsonNetworkEntity], raise_exception: bool = True) -> Optional[EntityWrapper]:
        try:
            entity = self.get_entity(entity)
        except Exception as e:
            if raise_exception:
                raise e
            return None

        if entity.entity_id in self._wrappers:
            return self._wrappers[entity.entity_id]
        if raise_exception:
            raise NetworkNodeNotFoundException(f"No wrapper for entity {entity.entity_id} found")
        return None

    def is_running(self) -> bool:
        return self._started.is_set()

    def add_node(self, node: WattsonNetworkNode) -> WattsonNetworkNode:
        # Create wrapper
        if isinstance(node, WattsonNetworkDockerHost):
            wrapper = DockerWrapper(entity=node, emulator=self)
            self._wrappers[node.entity_id] = wrapper
        elif isinstance(node, WattsonNetworkVirtualMachineHost):
            wrapper = VirtualMachineWrapper(entity=node, emulator=self)
            self._wrappers[node.entity_id] = wrapper
        elif isinstance(node, WattsonNetworkHost):
            wrapper = NativeWrapper(entity=node, emulator=self)
            self._wrappers[node.entity_id] = wrapper
        elif isinstance(node, WattsonNetworkSwitch):
            wrapper = OvsWrapper(entity=node, emulator=self)
            self._wrappers[node.entity_id] = wrapper
        else:
            raise RuntimeError(f"Unknown node type: {node.__class__.__name__}")
        super().add_node(node)
        if self.is_running():
            wrapper.create()
            node.start()
        return node

    def add_interface(self, node: Union[str, WattsonNetworkNode], interface: WattsonNetworkInterface) -> WattsonNetworkInterface:
        wrapper = InterfaceWrapper(entity=interface, emulator=self)
        self._wrappers[interface.entity_id] = wrapper
        interface = super().add_interface(node, interface)
        if self.is_running():
            wrapper.create()
            interface.start()
        return interface
    
    def add_link(self, link: WattsonNetworkLink) -> WattsonNetworkLink:
        wrapper = LinkWrapper(entity=link, emulator=self, enable_link_properties=not self._disable_tc_link)
        self._wrappers[link.entity_id] = wrapper
        link = super().add_link(link)
        link.add_on_link_property_change_callback(self._on_link_property_change_callback)
        if self.is_running():
            wrapper.create()
            link.start()
        return link

    def on_entity_start(self, entity: WattsonNetworkEntity):
        if isinstance(entity, WattsonNetworkLink):
            wrapper = typing.cast(LinkWrapper, self.get_wrapper(entity))
            wrapper.apply_link_properties()

    def on_entity_remove(self, entity: WattsonNetworkEntity):
        super().on_entity_remove(entity=entity)
        self.logger.info(f"Removing {entity.entity_id} ({entity.__class__.__name__})")
        if not self.is_running():
            return
        if isinstance(entity, WattsonNetworkLink):
            """
            for interface in [entity.interface_a, entity.interface_b]:
                if interface is None:
                    continue
                interface.link = None
                if self.has_entity(interface):
                    self.remove_interface(interface)
            """
            pass
        if isinstance(entity, WattsonNetworkInterface):
            entity.node.remove_interface(entity)
        wrapper = self.get_wrapper(entity=entity)
        wrapper.clean()

    def on_entity_change(self, trigger_entity: WattsonNetworkEntity, change_name: str = "entity_changed"):
        if isinstance(trigger_entity, WattsonNetworkInterface):
            if change_name == "ip_address_set":
                self.logger.info(f"Updating IP address")
                wrapper = typing.cast(InterfaceWrapper, self.get_wrapper(trigger_entity))
                wrapper.update_ip_address()

    def _on_link_property_change_callback(self, link: WattsonNetworkLink, link_property: str, value: Any):
        wrapper = typing.cast(LinkWrapper, self.get_wrapper(link))
        wrapper.apply_link_properties()

    def get_namespace(self, node: Union[str, WattsonNetworkEntity], raise_exception: bool = True) -> Optional[Namespace]:
        wrapper = self.get_wrapper(entity=node, raise_exception=raise_exception)
        if wrapper is None:
            return None
        return wrapper.get_namespace()

    def get_additional_namespace(self, node: Union[str, WattsonNetworkEntity], raise_exception: bool = True) -> Optional[Namespace]:
        wrapper = self.get_wrapper(entity=node, raise_exception=raise_exception)
        if wrapper is None:
            return None
        return wrapper.get_additional_namespace()

    def has_additional_namespace(self, node: Union[str, WattsonNetworkEntity], raise_exception: bool = True) -> bool:
        return self.get_additional_namespace(node=node, raise_exception=raise_exception) != self.get_namespace(node=node, raise_exception=raise_exception)

    def get_main_namespace(self) -> Namespace:
        return self._main_namespace

    def _adjust_resource_limits(self):
        try:
            limit = resource.getrlimit(resource.RLIMIT_NOFILE)
            resource.setrlimit(resource.RLIMIT_NOFILE, (limit[1], limit[1]))
            resource.setrlimit(resource.RLIMIT_AS, (resource.RLIM_INFINITY, resource.RLIM_INFINITY))
            resource.setrlimit(resource.RLIMIT_MEMLOCK, (resource.RLIM_INFINITY, resource.RLIM_INFINITY))
            self.logger.info(f"Adjusting limits (open file descriptors) from {limit[0]} to {limit[1]}")
        except Exception as e:
            self.logger.error(f"Could not adjust resource limits: {e=}")

        # ARP Cache Size
        # This is necessary to make sure routers and other hosts don't run in problems, e.g., during an nmap scan
        arp_cache_size = 4096 * 4
        self.logger.info(f"Adjusting arp cache size to {arp_cache_size}")
        subprocess.check_call(f"sysctl net.ipv4.neigh.default.gc_thresh1={arp_cache_size}", shell=True, stdout=subprocess.DEVNULL)
        subprocess.check_call(f"sysctl net.ipv4.neigh.default.gc_thresh2={2 * arp_cache_size}", shell=True, stdout=subprocess.DEVNULL)
        subprocess.check_call(f"sysctl net.ipv4.neigh.default.gc_thresh3={4 * arp_cache_size}", shell=True, stdout=subprocess.DEVNULL)

        # Reserve ports for Wattson-related servers
        reserved_ports = ["2404", "51000-51006"]
        self.logger.info(f"Reserving ports: {', '.join(reserved_ports)}")
        subprocess.check_call(f"sysctl net.ipv4.ip_local_reserved_ports={','.join(reserved_ports)}", shell=True, stdout=subprocess.DEVNULL)

        # ConnTrack
        conn_track_max = 524288
        self.logger.info(f"Adjusting connection track maximum limit to {conn_track_max}")
        subprocess.check_call(f"sysctl net.netfilter.nf_conntrack_max={conn_track_max}", shell=True, stdout=subprocess.DEVNULL)

        # Multicast Groups
        max_multicast_groups = 2048
        self.logger.info(f"Adjusting number of multicast group limit to {max_multicast_groups}")
        subprocess.check_call(f"sysctl net.ipv4.igmp_max_memberships={max_multicast_groups}", shell=True, stdout=subprocess.DEVNULL)

        # inotify Limit
        inotify_max = 1024
        self.logger.info(f"Adjusting inotify limit to {inotify_max}")
        subprocess.check_call(f"sysctl fs.inotify.max_user_instances={inotify_max}", shell=True, stdout=subprocess.DEVNULL)

        # Routing Limits
        max_send_window_size = 16 * 2**20
        max_receiver_window_size = 16 * 2**20
        route_garbage_collector = 2**18
        self.logger.info("Adjusting routing limits")
        subprocess.check_call(f"sysctl net.core.wmem_max={max_send_window_size}", shell=True, stdout=subprocess.DEVNULL)
        subprocess.check_call(f"sysctl net.core.rmem_max={max_receiver_window_size}", shell=True, stdout=subprocess.DEVNULL)
        subprocess.check_call(f"sysctl net.ipv4.xfrm4_gc_thresh={route_garbage_collector}", shell=True, stdout=subprocess.DEVNULL)

    def start(self):
        if not self._main_namespace.exists():
            self._main_namespace.from_pid(os.getpid())
        super().start()
        self._adjust_resource_limits()

        if self._async_start:
            self.logger.warning(f"Asynchronous start enabled - be aware of potential stability issues")
        else:
            self.logger.info("Using synchronous start. Reduced startup performance.")

        exception_lock: threading.Lock = threading.Lock()
        first_exception: Optional[Exception] = None

        def _entity_action(action: str, _entity: WattsonNetworkEntity, _logger, _progress_printer):
            nonlocal first_exception
            nonlocal exception_lock

            if action == "create":
                _wrapper = self.get_wrapper(entity=_entity)
                if not _wrapper.create():
                    _logger.error(f"Could not create {_entity.__class__.__name__} {_entity.entity_id}")
                    with exception_lock:
                        if first_exception is None:
                            first_exception = Exception(f"Could not create {_entity.__class__.__name__} {_entity.entity_id}")
                            return False
            elif action == "start":
                try:
                    _entity.start()
                except Exception as e:
                    _logger.error(f"Could not start {_entity.__class__.__name__} {_entity.entity_id}")
                    _logger.error(traceback.format_exc())
                    with exception_lock:
                        if first_exception is None:
                            first_exception = Exception(f"Could not start {_entity.__class__.__name__} {_entity.entity_id}")
                            return False
            else:
                _logger.error(f"Invalid action {action}")
                with exception_lock:
                    if first_exception is None:
                        first_exception = Exception(f"Invalid action {action}")
                        return False
            _progress_printer.inc()
            return True

        # Wrapper creation
        wrapper_types = [
            ("nodes", self.get_nodes()),
            ("interfaces", self.get_interfaces()),
            ("links", self.get_links()),
        ]
        for type_name, entities in wrapper_types:
            self.logger.info(f"Creating {type_name}")
            # Create wrappers
            progress_printer = ProgressPrinter(max_progress=len(entities), enable_print=self._print_progress, on_stop_margin=True)
            progress_printer.start()

            tasks = []
            for entity in entities:
                if self._async_start:
                    OvsWrapper.enable_batch()
                    tasks.append(("create", entity, self.logger, progress_printer))
                else:
                    if not _entity_action("create", entity, self.logger, progress_printer) and first_exception is not None:
                        raise first_exception

            if len(tasks) > 0:
                with multiprocessing.pool.ThreadPool(processes=min(self._async_threads, len(tasks))) as pool:
                    pool.starmap(_entity_action, tasks)

            if self._async_start:
                OvsWrapper.flush_batch()
                OvsWrapper.disable_batch()
            progress_printer.stop()

        self._started.set()

        # Start Wattson WattsonNetworkEntity instances
        self.logger.info(f"Starting entities")
        progress_printer = ProgressPrinter(max_progress=len(self.get_entities()), enable_print=self._print_progress, on_stop_margin=True)
        progress_printer.start()
        # threads = []
        tasks = []
        for entity in self.get_entities():
            if self._async_start:
                tasks.append(("start", entity, self.logger, progress_printer))
                #t = threading.Thread(target=_entity_action, args=("start", entity, self.logger, progress_printer))
                #threads.append(t)
                #t.start()
            else:
                if not _entity_action("start", entity, self.logger, progress_printer) and first_exception is not None:
                    raise first_exception

        if len(tasks) > 0:
            with multiprocessing.pool.ThreadPool(processes=min(self._async_threads, len(tasks))) as pool:
                pool.starmap(_entity_action, tasks)

        progress_printer.stop()

    def stop(self):
        super().stop()
        OvsWrapper.enable_batch()
        self._started.clear()
        self.stop_services()
        self.logger.info(f"Cleaning up interfaces")
        progress_printer = ProgressPrinter(max_progress=len(self.get_interfaces()), enable_print=self._print_progress, on_stop_margin=True)
        progress_printer.start()

        def _clean_wrapper(_wrapper, _progress_printer):
            _wrapper.clean()
            _progress_printer.inc()

        interface_threads = []

        for interface in self.get_interfaces():
            wrapper = self.get_wrapper(entity=interface)
            t = threading.Thread(target=_clean_wrapper, args=(wrapper, progress_printer), daemon=True)
            interface_threads.append(t)
            t.start()
        for t in interface_threads:
            t.join()
        OvsWrapper.flush_batch()
        progress_printer.stop()

        self.logger.info(f"Cleaning up nodes")
        progress_printer = ProgressPrinter(max_progress=len(self.get_nodes()), enable_print=self._print_progress, on_stop_margin=True)
        progress_printer.start()

        node_threads = []

        for node in self.get_nodes():
            wrapper = self.get_wrapper(entity=node)
            t = threading.Thread(target=_clean_wrapper, args=(wrapper, progress_printer), daemon=True)
            node_threads.append(t)
            t.start()
        for t in node_threads:
            t.join()
        self.logger.info(f"Flushing OVS")
        OvsWrapper.disable_batch()
        OvsWrapper.flush_batch()

        progress_printer.stop()
        self._main_namespace.clean()

    def deploy_services(self):
        self.logger.info("Starting services")
        services = []
        for node in self.get_nodes():
            if node.has_services():
                for service_id, service in node.get_services().items():
                    if service.autostart:
                        services.append(service)
        progress_printer = ProgressPrinter(max_progress=len(services), on_stop_margin=True, enable_print=self._print_progress, show_custom_prefix=True)
        progress_printer.start()
        longest_service_name_length = 0

        if len(services) > 0:
            longest_service_name_length = len(sorted(services, key=lambda s: len(s.name), reverse=True)[0].name)
            longest_service_name_length = min(longest_service_name_length, 30)

        for service in sorted(services, key=lambda s: s.get_priority().get_global(), reverse=True):
            progress_printer.set_custom_prefix(str(service.name).ljust(longest_service_name_length))
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

    def on_topology_change(self, trigger_entity: WattsonNetworkEntity, change_name: str = "topology_changed"):
        super().on_topology_change(trigger_entity, change_name)
        if not self.is_running:
            return
        with self._topology_change_lock:
            notification = WattsonNotification(
                notification_topic=WattsonNetworkNotificationTopic.TOPOLOGY_CHANGED,
                notification_data={"entity_id": trigger_entity.entity_id, "change": change_name}
            )
            self._topology_change_cache = notification
            if self._topology_change_timer is not None and self._topology_change_timer.is_alive():
                self._topology_change_timer.reset()
            else:
                self._topology_change_timer = ResettableTimer(3, self._flush_topology_change_notification)
                self._topology_change_timer.start()

    def _flush_topology_change_notification(self):
        with self._topology_change_lock:
            if self._topology_change_cache is None:
                return
            self.send_notification(notification=self._topology_change_cache)
            self._topology_change_cache = None
            self._topology_change_timer = None

    def open_browser(self, node: WattsonNetworkNode) -> bool:
        wrapper = self.get_wrapper(node)
        if isinstance(wrapper, (NativeWrapper, DockerWrapper)):
            # Find local user if any
            process = psutil.Process(os.getpid())
            user = None
            while process is not None and "sudo" not in process.cmdline():
                process = process.parent()
            if process is not None and process.parent() is not None:
                user = process.parent().username()

            binary = None
            browsers = ["firefox", "google-chrome", "chromium"]
            for browser in browsers:
                code0, lines = node.get_namespace().exec(["which", browser])
                if code0 and len(lines) == 1:
                    binary = lines[0]
                    break
            if binary is None:
                return False
            process = node.get_namespace().popen([binary], shell=True, as_user=user, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            node.manage_process(process)
            return process.poll() is None
        return False
