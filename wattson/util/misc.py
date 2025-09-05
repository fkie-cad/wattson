import importlib
import importlib.util
import inspect
import ipaddress
import hashlib
import pickle
import sys
import traceback
from pathlib import Path

import psutil, time
from subprocess import run
from typing import Union, Tuple, Dict, Any


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

    Args:
        ip_addr (Union[str, ipaddress.IPv4Address]):
            Passed IP address (either str or
        pref_len (int, optional):
            Assumed prefix length. defaults to 24. Also extracts from ip_addr if included.

    Returns:
        ipaddress.IPv4Network: subnet (in "ipaddress" format)
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
    Return the IPC Filename based on an IP address and a Port usually used for TCP communication.

    Args:
        ip_addr (str):
            The IP address
        port (int):
            The Port (former TCP Port)
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


def dynamic_get_classes_from_file(file: Path) -> list:
    try:
        spec = importlib.util.spec_from_file_location("dynamic.loader", file)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        classes = []
        for name, obj in inspect.getmembers(module):
            if inspect.isclass(obj):
                if obj.__module__ == "dynamic.loader":
                    classes.append(obj)
        return classes
    except Exception as e:
        traceback.print_exception(e)
        return []


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


def deep_update(mapping: Dict[Any, Any], *updating_mappings: Dict[Any, Any]) -> Dict[Any, Any]:
    updated_mapping = mapping.copy()
    for updating_mapping in updating_mappings:
        for k, v in updating_mapping.items():
            if k in updated_mapping and isinstance(updated_mapping[k], dict) and isinstance(v, dict):
                updated_mapping[k] = deep_update(updated_mapping[k], v)
            else:
                updated_mapping[k] = v
    return updated_mapping
