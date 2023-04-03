import time

from ipmininet.link import IPIntf


class WattsonIPInterface(IPIntf):
    def __init__(self, name, node=None, port=None, link=None,
                 mac=None, **params):
        self.args = []
        self.default_route = None
        self.config_change_start_end = {}
        super().__init__(name, node, port, link, mac, **params)

    def ifconfig(self, *args):
        self.args.append(args)
        routes = self.cmd("ip route").split(" \r\n")
        if len(routes) > 0 and "default" in routes[0]:
            self.default_route = routes[0]
        result = super().ifconfig(*args)
        if args == (("up",)) and self.default_route:
            addresses = self.cmd("ip addr").split("\n")
            interface_names = {i: r for i, r in enumerate(addresses)}
            if len(interface_names) > 7:
                interface_name = interface_names[6].split(":")
                if len(interface_name) > 1:
                    x = interface_name[1].split("@")
                    if len(x) > 0:
                        self.cmd(f"ip link set {x[0]} up")
                        self.cmd("ip route add " + self.default_route + " onlink")
        return result

    def config(self, bw=None, delay=None, jitter=None, loss=None,
               gro=False, txo=True, rxo=True,
               speedup=0, use_hfsc=False, use_tbf=False,
               latency_ms=None, enable_ecn=False, enable_red=False,
               max_queue_size=None, **params):
        default_route = self.cmd("ip route").split(" \r\n")[0]
        self.config_change_start_end["start"] = time.time()
        result = super().config(bw, delay, jitter, loss,
                                gro, txo, rxo,
                                speedup, use_hfsc, use_tbf,
                                latency_ms, enable_ecn, enable_red,
                                max_queue_size, **params)
        self.config_change_start_end["end"] = time.time()
        self.cmd("ip route add " + default_route)
        return result
