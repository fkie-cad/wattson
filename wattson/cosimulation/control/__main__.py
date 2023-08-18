import os
import signal
import sys
import traceback

from wattson.cosimulation.control.co_simulation_controller import CoSimulationController
from wattson.cosimulation.control.argument_parser import get_argument_parser


def main():
    parser = get_argument_parser()
    args = parser.parse_args()
    config = {}

    if args.artifact_directory is not None:
        config["working_dir_base"] = args.artifact_directory
        config["create_working_dir_hierarchy"] = False
        config["create_working_dir_symlink"] = False

    from wattson.cosimulation.simulators.network.emulators.wattson_network_emulator import WattsonNetworkEmulator
    controller = CoSimulationController(args.scenario,
                                        network_emulator=WattsonNetworkEmulator(),
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

    signal.signal(signalnum=signal.SIGTERM, handler=teardown)
    signal.signal(signalnum=signal.SIGINT, handler=teardown)

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
