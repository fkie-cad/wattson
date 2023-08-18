from ipmininet.host import IPHost
import mininet.node as _m
from ipmininet.utils import realIntfList

from mininet.log import debug

Docker = _m.Docker
Docker.__bases__ = (IPHost, )


class IPDocker(Docker, IPHost):
    def __init__(self, name, dimage=None, dcmd=None, build_params=None, **kwargs):
        build_params = {} if build_params is None else build_params
        Docker.__init__(self, name=name, dimage=dimage, dcmd=dcmd, build_params=build_params, **kwargs)
        IPHost.__init__(self, name=name, **kwargs)

    def add_default_route(self):
        for itf in realIntfList(self):
            for r in itf.broadcast_domain.routers:
                if self.use_v4 and len(r.addresses[4]) > 0:
                    cmd = f"ip route delete default; ip route add default dev {itf.name} via {r.ip}"
                    self.cmd(cmd)

    def create_default_routes(self):
        self.add_default_route()

    def _image_exists(self, repo, tag, _id=None):
        """
        Checks if the repo:tag image exists locally
        :return: True if the image exists locally. Else false.
        """
        images = self.dcli.images()
        imageTag = "%s:%s" % (repo, tag)
        for image in images:
            if image.get("RepoTags", None):
                if imageTag in image.get("RepoTags", []):
                    debug("Image '{}' exists.\n".format(imageTag))
                    return True
            if image.get("Id", None):
                if image.get("Id") == _id:
                    return True
        return False
