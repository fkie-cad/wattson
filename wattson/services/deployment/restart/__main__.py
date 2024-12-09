import subprocess
import sys
from pathlib import Path

import psutil
import argparse

from psutil import TimeoutExpired


def main():
    parser = argparse.ArgumentParser("Restart deployment")
    parser.add_argument("pid", type=int, help="The PID of the potentially running service")
    parser.add_argument("id")
    parser.add_argument("config")
    parser.add_argument("log", type=str, help="The path of the log file to use", default=None)

    args = parser.parse_args()

    # Check if process can be terminated
    if psutil.pid_exists(args.pid):
        p = psutil.Process(args.pid)
        if p.is_running():
            print(f"Stopping service with PID {args.pid}")
            # print(f"  {p.cmdline()}")
            p.terminate()
            try:
                p.wait(10)
            except TimeoutExpired:
                print("  Refused to terminate, trying to kill running service")
                p.kill()
                try:
                    p.wait(10)
                except TimeoutExpired:
                    print("Failed to stop service")
                    sys.exit(1)

    # Start process
    print("Starting service")
    kwargs = {}
    log_file_info = ""
    if args.log is not None:
        handle = Path(args.log).open("a")
        kwargs = {
            "stdout": handle,
            "stderr": handle,
            "close_fds": True
        }
        log_file_info = f"- log file is {args.log}"
    p = subprocess.Popen([sys.executable, "-m", "wattson.services.deployment", args.id, args.config], **kwargs)
    print(f"Service created with PID {p.pid} {log_file_info}")


if __name__ == "__main__":
    main()
