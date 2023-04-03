from ipmininet.ipnet import IPNet
from ipmininet.iptopo import IPTopo
import abc


class NetworkModificatorInterface(abc.ABC):
    @abc.abstractmethod
    def modify_network(self, net: IPNet, importer) -> IPNet:
        raise NotImplementedError


class TopologyModificatorInterface(abc.ABC):
    @abc.abstractmethod
    def modify_topology(self, topo: IPTopo, importer) -> IPNet:
        raise NotImplementedError()
