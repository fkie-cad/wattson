import datetime
import threading
import time
from pathlib import Path
from typing import Optional, List, Union, Type, Set

import yaml

import wattson.util
from wattson.cosimulation.cli.cli import CLI
from wattson.cosimulation.control.constants import SIM_CONTROL_ID, SIM_CONTROL_PORT, SIM_CONTROL_PUBLISH_PORT
from wattson.cosimulation.control.interface.wattson_client import WattsonClient
from wattson.cosimulation.control.interface.wattson_query_handler import WattsonQueryHandler
from wattson.cosimulation.control.messages.wattson_event import WattsonEvent
from wattson.cosimulation.control.messages.wattson_notification import WattsonNotification
from wattson.cosimulation.control.messages.wattson_query import WattsonQuery
from wattson.cosimulation.control.messages.wattson_response import WattsonResponse
from wattson.cosimulation.control.scenario_extension import ScenarioExtension
from wattson.cosimulation.control.interface.wattson_server import WattsonServer
from wattson.cosimulation.exceptions import InvalidScenarioException
from wattson.cosimulation.simulators.network.components.wattson_network_host import WattsonNetworkHost
from wattson.cosimulation.simulators.network.network_emulator import NetworkEmulator
from wattson.services.configuration import ConfigurationStore
from wattson.cosimulation.simulators.physical.empty_physical_simulator import EmptyPhysicalSimulator
from wattson.cosimulation.simulators.physical.physical_simulator import PhysicalSimulator
from wattson.cosimulation.simulators.simulator import Simulator
from wattson.time import WattsonTime
from wattson.util.misc import dynamic_load_class_from_file
from wattson.networking.namespaces.namespace import Namespace
from wattson.util.progress_printer import ProgressPrinter
from wattson.util.events.wait_event import WaitEvent


class CoSimulationController(WattsonQueryHandler):
    def __init__(self, scenario_path: Path, **kwargs):
        self.scenario_path = scenario_path
        self._config = {}
        self._config.update(kwargs)
        self._configuration_store = ConfigurationStore()
        self._wattson_time = WattsonTime()

        self._stopped = threading.Event()

        self.sim_control_query_socket_string = ""
        self.sim_control_publish_socket_string = ""

        self._network_emulator: Optional[NetworkEmulator] = kwargs.get("network_emulator", None)
        self._physical_simulator: PhysicalSimulator = EmptyPhysicalSimulator()
        self._simulation_control_server: Optional[WattsonServer] = None

        self.logger = wattson.util.get_logger("Wattson", "Wattson", use_context_logger=False)

        self._working_dir_base = Path(kwargs.get("working_dir_base", Path("wattson-artifacts")))
        self._create_working_dir_hierarchy = kwargs.get("create_working_dir_hierarchy", True)
        self._create_working_dir_symlink = kwargs.get("create_working_dir_symlink", True)
        self.working_directory = self._working_dir_base
        self._sim_control_host: Optional[WattsonNetworkHost] = None
        self._required_sim_control_clients = set()

    @property
    def network_emulator(self) -> NetworkEmulator:
        if self._network_emulator is None:
            from wattson.cosimulation.simulators.network.emulators.wattson_network_emulator import WattsonNetworkEmulator
            self._network_emulator = WattsonNetworkEmulator()
        return self._network_emulator

    @property
    def physical_simulator(self) -> PhysicalSimulator:
        return self._physical_simulator

    @property
    def simulation_control_server(self) -> WattsonServer:
        return self._simulation_control_server

    @property
    def configuration_store(self) -> ConfigurationStore:
        return self._configuration_store

    @staticmethod
    def get_wattson_client_config(inline: bool = True) -> dict:
        if inline:
            return {
                "query_socket": "!sim-control-query-socket",
                "publish_socket": "!sim-control-publish-socket",
            }
        else:
            return {
                "wattson_client_config": CoSimulationController.get_wattson_client_config(inline=True)
            }

    def start(self):
        """
        Starts the co-simulation by performing the following steps:
        * Creates the network emulation
        * Creates the physical process simulation
        * Starts the physical process simulation
        * Deploys network hosts / applications
        :return:
        """
        self._stopped.clear()
        self.logger.info(f"Creating working directory")
        start_time = time.perf_counter()

        self.working_directory = self._working_dir_base
        if self._create_working_dir_hierarchy:
            scenario_name = self.scenario_path.name
            utc_time = datetime.datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S")
            self.working_directory = self.working_directory.joinpath(f"{scenario_name}_{utc_time}")
            if self.working_directory.exists():
                raise FileExistsError(f"Working directory {self.working_directory} already exists")
            self.working_directory.mkdir(parents=True)
            self.logger.info(f"Working directory is {self.working_directory}")
            if self._create_working_dir_symlink:
                symlink = self._working_dir_base.joinpath("latest")
                previous_symlink = self._working_dir_base.joinpath("previous")
                previous_symlink.unlink(missing_ok=True)
                if symlink.exists():
                    symlink.rename(previous_symlink)
                symlink.unlink(missing_ok=True)
                self.logger.info(f"Creating symlink {symlink} to working directory")
                symlink.symlink_to(self.working_directory.relative_to(symlink.parent), target_is_directory=True)

        self.network_emulator.set_working_directory(self.working_directory)
        self.network_emulator.send_notification_handler = self.send_notification
        self.network_emulator.enable_management_network()

        self._sim_control_host = WattsonNetworkHost(id=SIM_CONTROL_ID, config={"role": "sim-control"})
        self.network_emulator.add_host(self._sim_control_host)
        sim_control_ip = self._sim_control_host.get_management_interface().ip_address
        sim_control_query_socket_string = f"tcp://{sim_control_ip}:{SIM_CONTROL_PORT}"
        sim_control_publish_socket_string = f"tcp://{sim_control_ip}:{SIM_CONTROL_PUBLISH_PORT}"
        sim_control_query_bind_string = f"tcp://0.0.0.0:{SIM_CONTROL_PORT}"
        sim_control_publish_bind_string = f"tcp://0.0.0.0:{SIM_CONTROL_PUBLISH_PORT}"

        self.sim_control_query_socket_string = sim_control_query_socket_string
        self.sim_control_publish_socket_string = sim_control_publish_socket_string

        self._configuration_store.register_configuration(
            "sim-control-query-socket",
            sim_control_query_socket_string
        )
        self._configuration_store.register_configuration(
            "sim-control-publish-socket",
            sim_control_publish_socket_string
        )

        self._configuration_store.register_configuration("scenario_path", str(self.scenario_path.absolute()))
        self.physical_simulator.send_notification_handler = self.send_notification
        self.physical_simulator.set_working_directory(self.working_directory)

        # Start Network Emulation
        self.logger.info("Starting Network Emulation")
        self.network_emulator.start()

        required_clients = []
        required_clients.extend(list(self.network_emulator.get_simulation_control_clients()))
        required_clients.extend(list(self.physical_simulator.get_simulation_control_clients()))
        self._required_sim_control_clients = set(required_clients)

        self._simulation_control_server = WattsonServer(
            co_simulation_controller=self,
            query_socket_string=sim_control_query_bind_string,
            publish_socket_string=sim_control_publish_bind_string,
            namespace=self._sim_control_host.get_namespace(),
            wattson_time=self._wattson_time
        )
        self._simulation_control_server.start()
        self._simulation_control_server.wait_until_ready()

        # Start Physical Simulator
        self.logger.info("Starting Physical Simulation")
        self._physical_simulator.start()
        if not self._physical_simulator.wait_until_ready(10):
            self.logger.warning("Physical simulator not ready after 10 seconds. Continuing.")
        else:
            self.logger.info("Physical Simulation ready")
        # Deploy host processes
        self.network_emulator.deploy_services()
        stop_time = time.perf_counter()
        startup_duration = stop_time - start_time
        self.logger.info(f"Startup took {startup_duration} s")
        # Wait for WattsonClients
        self._wait_for_wattson_clients()
        self._simulation_control_server.set_event(WattsonEvent.START)

    def stop(self):
        if self._stopped.is_set():
            return
        self.logger.info("Stopping simulation control server")
        if self._simulation_control_server is not None:
            self._simulation_control_server.stop()
        self.logger.info("Stopping network emulation")
        if self.network_emulator is not None:
            self.network_emulator.stop()
        self.logger.info("Stopping physical simulator")
        if self.physical_simulator is not None:
            self.physical_simulator.stop()
        self.logger.info("Goodbye")
        self._stopped.set()

    def join(self, timeout: Optional[float] = None) -> bool:
        return self._stopped.wait(timeout=timeout)

    def cli(self, cli_sig_int_handler: Optional = None):
        """
        Starts a command-line-interface for the co-simulation
        :return:
        """
        wattson_client = WattsonClient(
            query_server_socket_string=self.sim_control_query_socket_string,
            publish_server_socket_string=self.sim_control_publish_socket_string,
            namespace=self._sim_control_host.get_namespace(),
            client_name="CLI"
        )
        wattson_client.start()
        wattson_client.register()
        wattson_client.require_connection()
        self.logger.info(f"Client ready")
        cli = CLI(wattson_client, default_sig_int_handler=cli_sig_int_handler)
        cli.run()
        wattson_client.stop()

    def manual_scenario(self, network_emulator: NetworkEmulator, physical_simulator: PhysicalSimulator):
        self._network_emulator = network_emulator
        self._physical_simulator = physical_simulator
        self._physical_simulator.set_network_emulator(network_emulator)

        self._physical_simulator.set_configuration_store(self._configuration_store)
        self._network_emulator.set_configuration_store(self._configuration_store)
        self._physical_simulator.set_working_directory(self.scenario_path)
        self._network_emulator.set_working_directory(self.scenario_path)

    def load_scenario(self):
        self.logger.info("Loading scenario")
        scenario_type_file = self.scenario_path.joinpath("scenario-type")
        if not scenario_type_file.exists():
            raise InvalidScenarioException("No scenario-type file found")
        with scenario_type_file.open("r") as f:
            scenario_type = f.read().strip()

        self._configuration_store.register_configuration("scenario_path", str(self.scenario_path.absolute()))
        self._configuration_store.register_configuration("statistics", {})

        extensions = []
        extension_list_file = self.scenario_path.joinpath(self._config.get("extensions", "extensions.yml"))
        if extension_list_file.exists():
            with extension_list_file.open("r") as f:
                extension_list = yaml.load(f, Loader=yaml.SafeLoader)
            for extension_entry in extension_list:
                if isinstance(extension_entry, str):
                    from wattson.cosimulation.control.yaml_scenario_extension import YamlScenarioExtension
                    # Yaml-based extension
                    file = extension_list_file.parent.joinpath(extension_entry)
                    if not file.exists():
                        self.logger.error(f"Invalid extension: {extension_entry} (File not found)")
                    with file.open("r") as f:
                        yaml_extension_config = yaml.load(f, Loader=yaml.SafeLoader)
                    extension = YamlScenarioExtension(co_simulation_controller=self, **yaml_extension_config)
                    extensions.append(extension)
                    continue
                # Python-based extension
                file = extension_entry.pop("file")
                file_path = extension_list_file.parent.joinpath(file)
                class_name = extension_entry.pop("class")
                enabled = extension_entry.pop("enabled", True)
                if not enabled:
                    continue
                config = extension_entry
                extension_class = dynamic_load_class_from_file(file_path, class_name)
                if not issubclass(extension_class, ScenarioExtension):
                    self.logger.error(f"Extension {class_name} from {file} is not a valid scenario extension")
                else:
                    extension = extension_class(co_simulation_controller=self, **config)
                    extensions.append(extension)
        pre_physical_extensions = [extension for extension in extensions if extension.provides_pre_physical()]
        post_physical_extensions = [extension for extension in extensions if extension.provides_post_physical()]

        # Load configuration if any
        configuration_file = self.scenario_path.joinpath("configuration.yml")
        if configuration_file.exists():
            with configuration_file.open("r") as f:
                configuration = yaml.load(f, Loader=yaml.SafeLoader)
                self._config.update(configuration)

        # Create physical Simulator instance
        self._physical_simulator = PhysicalSimulator.from_scenario_type(scenario_type)
        self._physical_simulator.set_configuration_store(self._configuration_store)
        self.network_emulator.set_configuration_store(self._configuration_store)

        # Load Network Emulator
        self.network_emulator.load_scenario(self.scenario_path)

        # Extensions: Pre-Physical
        if len(pre_physical_extensions) > 0:
            self.logger.info("Executing pre-physical extensions")
            for extension in pre_physical_extensions:
                self.logger.info(f" Applying {extension.__class__.__name__}")
                extension.extend_pre_physical()

        # Load Physical Simulator
        self.physical_simulator.set_network_emulator(self.network_emulator)
        self.physical_simulator.load_scenario(self.scenario_path)

        # Extensions: Post-Physical
        if len(post_physical_extensions) > 0:
            self.logger.info("Executing post-physical extensions")
            for extension in post_physical_extensions:
                self.logger.info(f" Applying {extension.__class__.__name__}")
                extension.extend_post_physical()

    def get_simulators(self) -> List[Simulator]:
        return [self.network_emulator, self.physical_simulator]

    def get_controller_namespace(self) -> Optional[Namespace]:
        return self._sim_control_host.get_namespace()

    def handles_simulation_query_type(self, query: Union[WattsonQuery, Type[WattsonQuery]]) -> bool:
        return False

    def handle_simulation_control_query(self, query: WattsonQuery) -> Optional[WattsonResponse]:
        pass

    def add_simulation_control_client(self, client_id: str, required: bool = True):
        if required:
            self._required_sim_control_clients.add(client_id)

    def get_simulation_control_clients(self) -> Set[str]:
        return set()

    def send_notification(self, notification: WattsonNotification):
        if self.simulation_control_server is not None:
            self.simulation_control_server.broadcast(notification)

    def _wait_for_wattson_clients(self):
        self.logger.info("Waiting for clients to connect")
        required_clients = self._required_sim_control_clients
        clients_connected_event = threading.Event()
        progress_printer = ProgressPrinter(max_progress=len(required_clients), on_stop_margin=True, stop_event=clients_connected_event)

        def update_client_progress(_):
            progress_printer.set_progress(len(self._simulation_control_server.get_clients()))

        self._simulation_control_server.set_on_client_registration_callback(update_client_progress)
        progress_printer.start()

        wait_event = WaitEvent(timeout=60)
        wait_event.start()

        def log_missing_clients(as_warning: bool = False):
            total_clients_num = len(required_clients)
            current_clients = self._simulation_control_server.get_clients()
            current_clients_num = len(current_clients)
            method = self.logger.warning if as_warning else self.logger.info
            missing_clients = set(required_clients).difference(current_clients)
            method(f"{current_clients_num} / {total_clients_num} expected clients have registered. Waiting for: {'  '.join(missing_clients)}")

        while not wait_event.is_set():
            if clients_connected_event.wait(10):
                wait_event.complete()
            else:
                log_missing_clients()
        if not clients_connected_event.is_set():
            log_missing_clients(as_warning=True)
        else:
            self.logger.info("All clients connected")
