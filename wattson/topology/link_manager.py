import json
from typing import Optional, Union

from wattson.topology.constants import DEFAULT_NAMESPACE


class LinkManager:
    def __init__(self, importer: 'NetworkManager'):
        self.importer = importer

    def addLink(self, iface1: str, iface2: str, **kwargs):
        lid = self.importer.utils.gen_link_id()
        link = {}
        link.update(kwargs)
        link["id"] = lid
        link["interfaces"] = [iface1, iface2]
        link["namespace"] = link.get("namespace", DEFAULT_NAMESPACE)
        self.importer.graph["links"].append(link)
        return lid

    def get_link_from_id(self, link_id: str) -> Optional[dict]:
        link_name = self.get_linkname(link_id)
        for link in self.importer.get_links():
            if link["id"] == link_name:
                return link
        return None

    def get_net_links(self):
        return self.importer.mininet_manager.get_mininet().links

    def link_in_namespace(self, link: dict, namespace: Optional[str] = None) -> bool:
        if namespace is None:
            namespace = self.importer.namespace
        return self.get_link_namespace(link) == namespace

    def get_link_namespace(self, link: dict) -> str:
        if "namespace" in link:
            return link["namespace"]
        return DEFAULT_NAMESPACE

    def find_links(self, host1, host2=None):
        if host2:
            node1 = self.importer.host_manager.get_net_host(host1)
            node2 = self.importer.host_manager.get_net_host(host2)
            return [intf[0] for intf in node1.connectionsTo(node2)]
        else:
            node1 = self.importer.host_manager.get_net_host(host1)
            return [interface for interface in node1.intfs.values()]

    def get_linkname(self, link: Union[str, dict]) -> str:
        if type(link) == dict:
            if "id" in link:
                link = link["id"]
        if self.importer.get_prefix_linknames():
            return f"l{link}"
        return link

    def get_net_link(self, link_id):
        if link_id in self.importer.get_link_map():
            return self.importer.get_link_map()[link_id]
        # Lazy search
        link = self.get_link_from_id(link_id)
        if link is None:
            raise ValueError(f"Link {link_id} could not be found")
        iface_left = link["interfaces"][0]
        iface_right = link["interfaces"][1]
        hid_left, iid_left = self.importer.utils.parse_interface(iface_left)
        hid_right, iid_right = self.importer.utils.parse_interface(iface_right)
        hn_left = self.importer.host_manager.ghn(hid_left)
        hn_right = self.importer.host_manager.ghn(hid_right)
        links = self.get_net_links()
        for link in links:
            if link.intf1.node.name == hn_left and link.intf2.node.name == hn_right:
                self.importer.get_link_map()[link_id] = link
                return link
            if link.intf2.node.name == hn_left and link.intf1.node.name == hn_right:
                self.importer.get_link_map()[link_id] = link
                return link
        return None

    def set_link_status(self, link_id: str, status: str):
        link_id = link_id[1:] if self.importer._prefix_linknames else link_id
        net_link = self.get_net_link(link_id=link_id)
        net_link.intf1.ifconfig(status)
        net_link.intf2.ifconfig(status)

    def set_link_parameter(self, link_id: str, parameter: str, new_value):
        if parameter == "bw" or parameter == "bandwidth":
            new_value = self.importer.mininet_manager.parse_bandwidth(new_value)
            parameter = "bw"
        link_id = link_id[1:] if self.importer._prefix_linknames else link_id
        net_link = self.get_net_link(link_id=link_id)

        interface_left = net_link.intf1
        interface_right = net_link.intf2
        old_left_params = interface_left.params
        old_right_params = interface_right.params
        old_left_params[parameter] = new_value
        old_right_params[parameter] = new_value
        res_left = interface_left.config(**old_left_params)
        res_right = interface_right.config(**old_right_params)
        json.dump(
            {
                "left": interface_left.config_change_start_end,
                "right": interface_right.config_change_start_end
            },
            (self.importer.artifacts_dir / f"link_{link_id}_intf_change_times.txt").open("w+")
        )
        if res_left["tcoutputs"] == ['', '', '', ''] and res_right["tcoutputs"] == ['', '', '', '']:
            link = self.get_link_from_id(link_id)
            link[parameter] = new_value

    def remove_link(self, link_id: str):
        link = self.get_link_from_id(link_id)
        iface1 = self.importer.host_manager.get_net_host(self.importer.utils.parse_interface(link["interfaces"][0])[0])
        iface2 = self.importer.host_manager.get_net_host(self.importer.utils.parse_interface(link["interfaces"][1])[0])
        self.importer.mininet_manager.get_mininet().delLinkBetween(iface1, iface2)

    def stop_link(self, link_id: str):
        net_link = self.get_net_link(link_id=link_id)
        net_link.stop()

    def decide_linkname_prefix_requirement(self):
        return
        """for node_id in self.importer.graph["nodes"]:
            if node_id[0].isdigit():
                self.importer._prefix_linknames = True
                return
        """

    def graph_add_tap_links(self, graph):
        counter = 0
        for nid, node in graph["nodes"].items():
            if "tap_interfaces" not in node:
                continue
            for interface in node["tap_interfaces"]:
                if interface.get("type") == "client" and "target" in interface:
                    link = {
                        "id": f"auto-tap-{counter}",
                        "interfaces": [
                            f"{nid}.{interface['id']}",
                            interface["target"]
                        ]
                    }
                    graph["links"].append(link)
                    counter += 1
        return
