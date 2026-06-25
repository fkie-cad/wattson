import logging
from typing import Optional, List, Dict

from pyroute2 import IPRoute

from wattson.networking.namespaces.namespace import Namespace


class LinuxNamespace(Namespace):
    def __init__(self, name: str, logger: Optional[logging.Logger] = None):
        super().__init__(name, logger)
        self._pyroute_ns: IPRoute = None
        self._interface_id_map: Dict[str, int] = {}

    def __del__(self):
        if self._pyroute_ns is not None:
            self._pyroute_ns.close()
            self._pyroute_ns = None

    def create(self, clean: bool = True) -> bool:
        res = super().create(clean)
        return res

    def clean(self) -> bool:
        res = super().clean()
        return res

    def _ensure_pyroute(self):
        if self._pyroute_ns is None:
            if self.exists():
                self._pyroute_ns = IPRoute(netns=self.name)
            else:
                raise RuntimeError("Cannot use PyRoute in non-existent namespace")

    def if_get_index(self, interface_name: str) -> Optional[int]:
        if interface_name not in self._interface_id_map:
            self._ensure_pyroute()
            try:
                if_index = self._pyroute_ns.link_lookup(ifname=interface_name)[0]
                if if_index is not None:
                    self._interface_id_map[interface_name] = if_index
            except IndexError:
                return None
            except Exception as e:
                self.logger.error(f"{interface_name}: {repr(e)}")
                return None
        return self._interface_id_map.get(interface_name)

    def if_get_info(self, interface_name: str):
        self._ensure_pyroute()
        try:
            return self._pyroute_ns.link_lookup(ifname=interface_name)
        except:
            return False

    def if_add(self, interface_name: str, interface_type: str) -> bool:
        self._ensure_pyroute()
        try:
            self._pyroute_ns.link("add", ifname=interface_name, kind=interface_type)
        except:
            return False
        return True

    def if_delete(self, interface_name: str) -> bool:
        self._ensure_pyroute()
        try:
            if_index = self.if_get_index(interface_name)
            self._interface_id_map.pop(interface_name, None)
            self._pyroute_ns.link("delete", index=if_index)
        except:
            return False
        return True

    def if_set_namespace(self, interface_name: str, namespace: str) -> bool:
        self._ensure_pyroute()
        try:
            self._pyroute_ns.link("set", index=self.if_get_index(interface_name), net_ns_fd=namespace)
        except:
            return False
        return True

    def if_up(self, interface_name: str) -> bool:
        self._ensure_pyroute()
        try:
            self._pyroute_ns.link("set", index=self.if_get_index(interface_name), state="up")
        except:
            return False
        return True

    def if_down(self, interface_name: str) -> bool:
        self._ensure_pyroute()
        try:
            self._pyroute_ns.link("set", index=self.if_get_index(interface_name), state="down")
        except:
            return False
        return True

    def if_flush_ip(self, interface_name: str) -> bool:
        self._ensure_pyroute()
        try:
            self._pyroute_ns.flush_addr(index=self.if_get_index(interface_name))
        except:
            return False
        return True

    def if_set_ip(self, interface_name: str, ip_address: str) -> bool:
        self._ensure_pyroute()
        try:
            self._pyroute_ns.addr("add", index=self.if_get_index(interface_name), address=ip_address)
        except:
            return False
        return True

    def if_set_mac(self, interface_name: str, mac_address: str) -> bool:
        self._ensure_pyroute()
        self.logger.info(f"Setting {interface_name} MAC to {mac_address}")
        try:
            self._pyroute_ns.link("set", index=self.if_get_index(interface_name), address=mac_address)
        except:
            return False
        return True

    def if_rename(self, interface_name_old: str, interface_name_new: str) -> bool:
        self._ensure_pyroute()
        try:
            self._pyroute_ns.link("set", index=self.if_get_index(interface_name_old), name=interface_name_new)
        except:
            return False
        return True

    def _if_parse_info(self, info: dict) -> dict:
        ip_addresses = []
        if "ifindex" in info:
            return super()._if_parse_info(info)
        try:
            info["attrs"] = dict(info["attrs"])
            attrs = info["attrs"]
        except KeyError:
            self.logger.error(repr(info))
            raise

        for address_info in info.get("addr_info", []):
            addr_type = "inet"
            if address_info.get("family") == "inet":
                addr_type = "ipv4"
            elif address_info.get("family") == "inet6":
                addr_type = "ipv6"
            ip_addresses.append({
                "ip-address-type": addr_type,
                "ip-address": address_info.get("local"),
                "prefix": address_info.get("prefixlen")
            })
        return {
            "name": attrs.get("IFLA_IFNAME"),
            "ip-addresses": ip_addresses,
            "statistics": {},
            "hardware-address": attrs.get("IFLA_ADDRESS"),
            "state": info.get("state", "unknown"),
            "index": info.get("index"),
        }

    def if_list_existing(self, try_num: int = 0, max_tries: int = 10, retry_error_message: str = "") -> List[Dict]:
        self._ensure_pyroute()
        links = [self._if_parse_info(l) for l in self._pyroute_ns.get_links()]
        return links
