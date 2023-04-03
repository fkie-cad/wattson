import importlib
import importlib.util
import ipaddress
import hashlib
import os
import pickle
import shutil
import sys
from pathlib import Path

import psutil, time
from subprocess import run
from typing import Union, Tuple


def disable_checksum_offloading():
    #  netifaces not available by default on latest Debian, only Ubuntu >= 18.04
    ifaces = [iface for iface in psutil.net_if_addrs() if 'eth' in iface]
    for iface in ifaces:
        args1 = (
            'ethtool', '--offload', iface,
            'rx', 'off',
            'tx', 'off',
        )
        args2 = (
            'ethtool', '-K', iface,
            'gso', 'off'
        )
        run(args1, check=True)
        run(args2, check=True)


def get_subnet(ip_addr: Union[str, ipaddress.IPv4Address],
               pref_len: int = 24) -> ipaddress.IPv4Network:
    """
    Return subnet of a given IP address. Uses the stdlib package "ipaddress".
    :param ip_addr: Passed IP address (either str or
    :param pref_len: Assumed prefix length. defaults to 24. Also extracts
    from ip_addr if included.
    :return: subnet (in "ipaddress" format)
    """
    if isinstance(ip_addr, ipaddress.IPv4Address):
        ip_addr = str(ip_addr)
    if "/" not in ip_addr:
        ip_addr = "{}/{}".format(ip_addr, pref_len)
    subnet = ipaddress.ip_network(ip_addr, strict=False)
    return subnet


def wait_for_cpu(max_load: int = 60):
    cpu_percent = psutil.cpu_percent()
    slept = False
    while cpu_percent > max_load:
        print(f"CPU usg too high ({cpu_percent}%), waiting..", end=' ',
              flush=True)
        time.sleep(cpu_percent / 150)
        cpu_percent = psutil.cpu_percent()
        slept = False
        while cpu_percent > max_load:
            print(f"CPU usg too high ({cpu_percent}%), waiting..", end=' ',
                  flush=True)
            time.sleep(cpu_percent / 150)
            cpu_percent = psutil.cpu_percent()
            slept = True
        if slept:
            print()


def get_zmqipc(ip_addr: str, port: int):
    """
    Return the IPC Filename based on an IP address and a Port usually used
    for TCP communication.
    :param ip_addr: The IP address
    :param port: The Port (former TCP Port)
    :return ZMQ IPC File in /tmp/
    """
    iphash = hashlib.sha1(ip_addr.encode()).hexdigest()[:8]
    porthash = hashlib.sha1(str(port).encode()).hexdigest()[:8]
    return "ipc:///tmp/zmq_{}_{}".format(iphash, porthash)


def dynamic_load_class_from_file(file: Path, class_name: str):
    spec = importlib.util.spec_from_file_location("dynamic.loader", file)
    script = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(script)
    o_cls = getattr(script, class_name)
    return o_cls


def dynamic_load_method_from_file(file: Path, method_name: str):
    return dynamic_load_class_from_file(file, method_name)


def dynamic_load_class(classpath: str):
    split = classpath.split(".")
    module = ".".join(split[:-1])
    cls = split[-1]
    try:
        module = importlib.import_module(module)
    except Exception:
        raise RuntimeError(f"Cannot Import Modifier Module {module}")

    ocls = getattr(module, cls)
    return ocls


def get_console_and_shell(pid: int, allow_missing: bool = False):
    wattson_process = psutil.Process(pid)
    known_terminals = ["xterm", "kitty", "gnome-terminal", "gnome-terminal-server", "konsole"]
    known_shells = ["sh", "ksh", "bash", "csh", "tcsh", "fish"]
    terminal = None
    shell = None
    for p in wattson_process.parents():
        if p.name() in known_terminals:
            terminal = p.exe()
        elif p.name() in known_shells:
            shell = p.exe()
        if terminal is not None and shell is not None:
            break
    if terminal is None:
        terminal = "xterm"
    if "gnome-terminal-server" in terminal:
        terminal = shutil.which("gnome-terminal")
    if shell is None:
        shell = "/bin/bash"
    if shutil.which(shell) is None:
        if allow_missing:
            shell = False
        else:
            print("Could not find a shell to execute")
            return False
    if shutil.which(terminal) is None:
        if allow_missing:
            terminal = False
        else:
            print("Could not find a terminal to open")
            return False
    return terminal, shell


def get_object_size(_element) -> Tuple[float, str]:
    _pickled = pickle.dumps(_element)
    size = sys.getsizeof(_pickled)
    unit = "B"
    s = 1024
    if s ** 2 > size >= s:
        unit = "KiB"
        size /= s
    elif size >= s ** 2:
        unit = "MiB"
        size /= s ** 2
    return size, unit
