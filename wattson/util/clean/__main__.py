import json
import re
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

import psutil
import os
from wattson.util.compat import fix_iptc
from wattson.networking.namespaces.namespace import Namespace

#fix_iptc()
#from mininet import clean


def clean_processes():
    print("Cleaning up WATTSON")
    python_processes = []
    wattson_processes = []
    arpspoof_processes = []
    routing_processes = []
    for proc in psutil.process_iter():
        if "python3" in proc.name() or "python" in proc.name():
            python_processes.append(proc)
        if "wattson " in proc.name() or "wattson." in proc.name():
            wattson_processes.append(proc)
        if "arpspoof" in proc.name():
            arpspoof_processes.append(proc)
        if "zebra" in proc.name() or "ospfd" in proc.name():
            routing_processes.append(proc)

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
            if "wattson." in c or "wattson " in c:
                kill(p)
                break
    print(f"  ArpSpoof...")
    for p in arpspoof_processes:
        kill(p)
    print("  Routing...")
    for p in routing_processes:
        kill(p)
    print("")
    print("Temporary files")
    for file in Path("/tmp").glob("wattson_*"):
        if file.is_file():
            try:
                file.unlink()
            except Exception as e:
                print(f"  Cannot unlink {file.absolute()}: {e=}")
        if file.is_dir():
            subprocess.run(["umount", "-f", str(file.absolute())], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
            try:
                shutil.rmtree(file)
            except Exception as e:
                print(f"  Cannot remove {file.absolute()}: {e=}")


def clean_docker():
    import docker
    client = docker.from_env()
    print("Docker containers...")
    for container in client.containers.list(all=True):
        if "wattson." in container.attrs["Name"]:
            print(f"   {container.attrs['Name']}...")
            container.remove(force=True)


def _clear_ovs_bridge(bridge):
    tries = 5
    success = False
    while not success and tries > 0:
        ret = subprocess.run(f"ovs-vsctl --if-exists del-br {bridge}", shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        if ret.returncode == 0:
            success = True
            break
        time.sleep(0.3)
        tries -= 1
    if not success:
        print(f"\nCould not delete bridge {bridge}", flush=True)
    else:
        print(f"{bridge}", end="  ", flush=True)


def _clear_ovs_bridges(bridges):
    parts = ["ovs-vsctl"]
    for bridge in bridges:
        parts.extend(["--if-exists", "del-br", bridge])



def clean_network():
    print(f"Cleaning up Wattson Network")
    print("  Switches")
    bridges = subprocess.check_output("ovs-vsctl --timeout=1 list-br", shell=True).decode().strip().splitlines()
    print(f"    Found {len(bridges)} switches")
    threads = []
    for bridge in bridges:
        t = threading.Thread(target=_clear_ovs_bridge, args=(bridge,), daemon=True)
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    if len(bridges):
        print("")
    # clean.cleanup()
    print("  Removing Wattson interfaces")
    json_links = subprocess.check_output("ip --json link show", shell=True)
    links = json.loads(json_links)
    # Delete blocks of links
    n = 1000  # chunk size
    link_names = [link["ifname"] for link in links]
    wattson_link_pattern = re.compile("^[a-z]+[0-9]+-(eth|mgm|mir|phy|tap)[0-9]+")
    wattson_links = [link for link in link_names if wattson_link_pattern.match(link)]
    print(", ".join(wattson_links))
    for i in range(0, len(wattson_links), n):
        cmd = '; '.join('ip link del %s' % link for link in wattson_links[i: i + n])
        subprocess.check_output(cmd, shell=True)
    print("")


def clean_namespaces():
    print("Wattson-related Namespaces")
    namespaces = Namespace.get_namespaces()
    for namespace in namespaces:
        if namespace.name.startswith("w_"):
            print(f"  Cleaning {namespace.name}")
            namespace.clean()


def main():
    if os.geteuid() != 0:
        print("Cleanup requires root privileges")
        sys.exit(1)
    clean_processes()
    clean_docker()
    clean_network()
    clean_namespaces()


if __name__ == '__main__':
    main()
