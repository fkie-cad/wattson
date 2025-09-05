import json
import os
import signal
import sys
import traceback
from pathlib import Path

import pyprctl
from wattson.cosimulation.control.co_simulation_controller import CoSimulationController
from wattson.cosimulation.control.argument_parser import get_argument_parser
from wattson.exceptions.invalid_argument_exception import InvalidArgumentException


def main():
    parser = get_argument_parser()
    args = parser.parse_args()
    config = {}

    pyprctl.set_name("W/Main")

    if args.artifact_directory is not None:
        config["working_dir_base"] = args.artifact_directory
        config["create_working_dir_hierarchy"] = False
        config["create_working_dir_symlink"] = False
    if len(args.export_notification_topic) > 0:
        config["export_notifications"] = args.export_notification_topic

    config["configuration"] = {}

    if args.ccx_export is not None:
        config["configuration"]["ccx_export"] = {
            "enabled": True,
            "file": Path(args.ccx_export)
        }

    for option, value in args.option:
        option_path = option.split(".")
        parsed_value = json.loads(value)
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

    config["configuration"]["vcc_export"] = []
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

    def teardown(_sig, _frame):
        try:
            controller.stop()
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

    controller.network_emulator.enable_management_network()
    controller.load_scenario()
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
        controller.stop()
    else:
        controller.logger.info("No terminal detected - disabling CLI")
        try:
            controller.join()
        except Exception:
            pass
        finally:
            controller.stop()
    print("Wattson process terminating")


if __name__ == '__main__':
    main()
