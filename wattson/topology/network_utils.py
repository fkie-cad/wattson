import ipaddress
import json
import shlex
from typing import Optional
import subprocess
import time


def wait_for_interface(interface: str, timeout: Optional[float] = None, poll_interval: float = 0.2,
                       namespace: Optional[str] = None) -> bool:
    """
    Waits for a physical interface to be ready and up.
    :param interface: The interface name to wait for
    :param timeout: An optional timeout in seconds
    :param poll_interval: The delay in seconds between each polling attempt. Defaults to 200ms
    :param namespace: The optional networking namespace to search for the interface
    :return: True iff the interface is up after the timeout
    """
    start = time.time()
    end = None if timeout is None else start + timeout
    ct = start
    while ct <= end:
        dev = get_interface_info(interface, namespace)
        if dev is not None:
            return True
        ct = time.time()
        next_sleep = max(poll_interval, end-ct)
        if next_sleep <= 0:
            return False
        time.sleep(next_sleep)
    return False


def get_node_interfaces(node) -> list:
    cmd = f"ip -j a show"
    res = node.cmd(cmd)
    interfaces = []
    try:
        i_faces = json.loads(res)
        for i_info in i_faces:
            ipv4 = None
            if "addr_info" in i_info:
                for a_info in i_info["addr_info"]:
                    if a_info["family"] == "inet":
                        ipv4 = a_info["local"]
                        ipv4 = ipaddress.IPv4Address(ipv4)
            interfaces.append({
                "name": i_info["ifname"],
                "ip": ipv4
            })
    finally:
        return interfaces


def get_interface_info(interface: str, namespace: Optional[str] = None) -> Optional[dict]:
    cmd = f"ip -j link show"
    if namespace is not None:
        cmd = f"ip netns exec {namespace} {cmd}"
    p = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    out, err = p.communicate()
    if p.returncode != 0:
        return None
    try:
        devices = json.loads(out)
        for dev in devices:
            if dev.get("ifname", None) == interface:
                return dev
    finally:
        pass
    return None
