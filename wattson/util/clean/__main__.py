import sys

import psutil
import os
from wattson.util.compat import fix_iptc
from wattson.util.namespace import Namespace

fix_iptc()
from ipmininet import clean


def main():
    if os.geteuid() != 0:
        print("Cleanup requires root privileges")
        sys.exit(1)
    print("Cleaning up WATTSON")
    python_processes = []
    wattson_processes = []
    arpspoof_processes = []
    for proc in psutil.process_iter():
        if "python3" in proc.name() or "python" in proc.name():
            python_processes.append(proc)
        if "wattson" in proc.name():
            wattson_processes.append(proc)
        if "arpspoof" in proc.name():
            arpspoof_processes.append(proc)

    def kill(process):
        print(f"   Killing '{' '.join(process.cmdline())}'")
        process.kill()

    print(f"  Wattson main process...")
    for p in wattson_processes:
        if p.pid == os.getpid():
            continue
        kill(p)
    print(f"  Wattson-related Python3 processes...")
    for p in python_processes:
        if p.pid == os.getpid():
            continue
        for c in p.cmdline():
            if "wattson" in c:
                kill(p)
                break
    print(f"  ArpSpoof...")
    for p in arpspoof_processes:
        kill(p)
    print("")
    print(f"Cleaning up Mininet")
    clean.cleanup()
    print("")
    print("Wattson-related Namespaces")
    namespaces = Namespace.get_namespaces()
    for namespace in namespaces:
        if namespace.name.startswith("w_"):
            print(f"  Cleaning {namespace.name}")
            namespace.clean()


if __name__ == '__main__':
    main()
