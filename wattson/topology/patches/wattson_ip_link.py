
from ipmininet.link import IPLink
from wattson.topology.patches.wattson_ip_interface import WattsonIPInterface

class WattsonIPLink(IPLink):
    def __init__(self, node1: str, node2: str, intf = WattsonIPInterface,
                 *args, **kwargs):
        intf = WattsonIPInterface
        super().__init__(node1, node2, intf, *args, **kwargs)
