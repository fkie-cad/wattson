
from mininet.link import Link
from wattson.networking.wattson_ip_interface import WattsonIPInterface


class WattsonIPLink(Link):
    def __init__(self, node1: str, node2: str, intf=WattsonIPInterface,
                 *args, **kwargs):
        intf = WattsonIPInterface
        super().__init__(node1, node2, intf=intf, *args, **kwargs)

    def delete(self):
        # Failsafe deletion method for links
        if self.intf1 is not None:
            self.intf1.delete()
            self.intf1 = None
        if self.intf2 is not None:
            self.intf2.delete()
            self.intf2 = None
