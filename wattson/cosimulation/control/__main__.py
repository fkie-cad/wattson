import json
import os
import signal
import subprocess
import sys
import traceback
from fileinput import close
from pathlib import Path

from wattson.util.threading import set_thread_name
from wattson.cosimulation.control.co_simulation_controller import CoSimulationController
from wattson.cosimulation.control.argument_parser import get_argument_parser
from wattson.exceptions.invalid_argument_exception import InvalidArgumentException


def main():
    parser = get_argument_parser()
    args = parser.parse_args()
    config = {}

    set_thread_name("W/Main")

    if args.artifact_directory is not None:
        config["working_dir_base"] = args.artifact_directory
        config["create_working_dir_hierarchy"] = False
        config["create_working_dir_symlink"] = False
    if len(args.export_notification_topic) > 0:
        config["export_notifications"] = args.export_notification_topic

    config["configuration"] = {}
    config["auto_pcap"] = args.pcap

    options = args.option

    if args.ccx_export is not None:
        config["configuration"]["ccx_export"] = {
            "enabled": True,
            "file": Path(args.ccx_export)
        }

    # Append TLS Options
    parsed_options = set()
    for arg in vars(args):
        if arg.startswith("tls."):
            tls_option = getattr(args, arg)
            if tls_option is not None:
                options.append((arg, tls_option))
                parsed_options.add(arg)

    # Parse options
    for option, value in options:
        option_path = option.split(".")
        if option not in parsed_options:
            parsed_value = json.loads(value)
        else:
            parsed_value = value
        if len(option_path) == 0:
            raise InvalidArgumentException(f"Option path is empty")
        c = config["configuration"]
        for segment in option_path[:-1]:
            c.setdefault(segment, {})
            c = c[segment]
        c[option_path[-1]] = parsed_value

    if args.clean:
        from wattson.util.clean.__main__ import main as clean
        clean()
        sys.exit(0)

    if args.physical_export:
        config["auto_export_enable"] = True
    if args.synchronous_start:
        config["async_start"] = False
    if args.vcc_proxy:
        config["vcc_proxy"] = True
    if args.statistics:
        config["enable_statistics"] = True

    config["configuration"]["vcc_export"] = []
    if args.vcc_export is None:
        args.vcc_export = ""
    if "m" in args.vcc_export or "measurement" in args.vcc_export:
        config["configuration"]["vcc_export"].append("measurement")
    if "e" in args.vcc_export or "estimation" in args.vcc_export:
        config["configuration"]["vcc_export"].append("estimation")

    # Clock
    clock = {
        "speed": args.clock_speed,
        "sim_clock_reference": args.sim_clock_reference,
        "wall_clock_reference": args.wall_clock_reference
    }
    config["clock"] = clock
    config["disable_link_properties"] = args.no_link_properties

    from wattson.cosimulation.simulators.network.emulators.wattson_network_emulator import WattsonNetworkEmulator
    network_emulator = WattsonNetworkEmulator(async_start=config.get("async_start", True), disable_link_properties=args.no_link_properties)
    if args.no_net:
        from wattson.cosimulation.simulators.network.emulators.empty_network_emulator import EmptyNetworkEmulator
        network_emulator = EmptyNetworkEmulator()
    elif args.empty_net:
        config["empty_network"] = True

    controller = CoSimulationController(args.scenario,
                                        network_emulator=network_emulator,
                                        **config)

    original_handlers = {
        signal.SIGINT: signal.getsignal(signalnum=signal.SIGINT),
        signal.SIGTERM: signal.getsignal(signalnum=signal.SIGTERM)
    }

    script_processes = []
    script_process_files = []

    def stop_script_processes():
        if len(script_processes) > 0:
            controller.logger.info(f"Stopping script processes")
        for script_process in script_processes:
            try:
                script_process.terminate()
            except Exception:
                controller.logger.info(f"Failed to terminate script process {script_process.pid}")

    def teardown(_sig, _frame):
        try:
            controller.stop()
            stop_script_processes()
            sys.exit(0)
        except Exception as e:
            controller.logger.warning(f"Error during teardown occurred - trying cleanup")
            controller.logger.error(f"{e=}")
            controller.logger.error(traceback.print_exception(e))
            try:
                from wattson.util.clean.__main__ import main as wattson_clean
                wattson_clean()
            finally:
                sys.exit(1)

    def interrupt(_sig, _frame):
        if controller.is_waiting_for_clients:
            controller.stop(attempt_wait_cancel=True)
        else:
            teardown(_sig, _frame)

    signal.signal(signalnum=signal.SIGTERM, handler=teardown)
    signal.signal(signalnum=signal.SIGINT, handler=interrupt)

    # Load Scenario
    controller.network_emulator.enable_management_network()
    controller.load_scenario()

    ###
    ### SCRIPTS
    ###
    python_exec = sys.executable
    script_log_dir = controller.working_directory.joinpath("script-logs")
    if len(args.script) > 0:
        script_log_dir.mkdir(parents=True, exist_ok=True)
    for script in args.script:
        script_path = Path(script)
        script_name = script_path.stem
        script_log_file = script_log_dir.joinpath(f"{script_name}.log")
        f = script_log_file.open(mode="w")
        cmd = [python_exec, script, ">", str(script_log_file.as_posix()), "2>&1"]
        cmd = [python_exec, script]  #, ">", str(script_log_file.as_posix()), "2>&1"]
        controller.logger.info(f"  Starting script {script_name} ({cmd})")
        p = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, preexec_fn=os.setpgrp, stdout=f, stderr=subprocess.STDOUT)
        script_processes.append(p)
        script_process_files.append(f)

    ###
    ### Start
    ###
    controller.start()

    show_cli = not args.no_cli
    if show_cli:
        try:
            os.get_terminal_size().columns
        except OSError:
            show_cli = False

    if show_cli:
        # Restore original sigint handler, such that CLI can handle KeyboardInterrupts
        # signal.signal(signalnum=signal.SIGINT, handler=original_handlers[signal.SIGINT])
        controller.logger.info(f"Starting CLI")
        controller.cli(cli_sig_int_handler=original_handlers[signal.SIGINT])
        stop_script_processes()
        controller.stop()
    else:
        controller.logger.info("No terminal detected - disabling CLI")
        try:
            controller.join()
        except Exception:
            pass
        finally:
            stop_script_processes()
            controller.stop()
    print("Wattson process terminating")


if __name__ == '__main__':
    main()
