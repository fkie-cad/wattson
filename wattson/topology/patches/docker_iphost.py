from docker.types import HostConfig
from ipmininet.host import IPHost
import mininet.node as _m
from ipmininet.utils import realIntfList

Docker = _m.Docker
Docker.__bases__ = (IPHost, )


class IPDocker(Docker, IPHost):
    def __init__(self, name, dimage=None, dcmd=None, build_params=None, **kwargs):
        #super().__init__(name, dimage, dcmd, build_params, **kwargs)
        build_params = {} if build_params is None else build_params
        Docker.__init__(self, name=name, dimage=dimage, dcmd=dcmd, build_params=build_params, **kwargs)
        IPHost.__init__(self, name=name, config=HostConfig, **kwargs)

    def add_default_route(self):
        for itf in realIntfList(self):
            for r in itf.broadcast_domain.routers:
                if self.use_v4 and len(r.addresses[4]) > 0:
                    cmd = f"ip route delete default; ip route add default dev {itf.name} via {r.ip}"
                    self.cmd(cmd)

    def create_default_routes(self):
        self.add_default_route()
