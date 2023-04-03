import abc
import typing
import pandas

InterfaceType = typing.Dict[str, typing.Union[str, typing.List[str]]]


class Device(abc.ABC):
    """
    Abstract class used for all devices.
    """
    def __init__(self, node_id: str,
                 interfaces: InterfaceType) -> None:
        # id of node (from SGAM); converted to string
        self.node_id = node_id
        # table with interfaces, dictionary where key is the target of the link
        # (id of the target node) and the value is this device's IP address if there
        # is such (as string) or a list of strings if there are multiple links
        self.interfaces = {}
        for k, v in interfaces.items():
            # convert to string names
            # store IP addresses as str and without subnetmask
            self.interfaces[str(k)] = str(v).split("/")[0]

    def to_dict(self):
        interfaces = []
        id = 1
        for dest,ip in self.interfaces.items():
            interface = {
                "id": f"i{id}",
                "destination_host": dest
            }
            if len(ip) > 0:
                interface["ip"] = ip
            interfaces.append(interface)
            id += 1

        return {
            "id": f"{self.node_id}",
            "interfaces": interfaces
        }

    def from_dict(self, d):
        self.node_id = d["id"]
        self.interfaces = d["interfaces"]


class SwitchDevice(Device):
    pass


class HostDevice(Device):
    """
    A device, that has exactly one IP address and a command which is executed
    to perform some task.
    """
    def __init__(self, node_id, interfaces, start_cmd=""):
        super().__init__(node_id, interfaces)
        self.start_cmd = start_cmd
        # True when this device is running / cmd has been executed
        self.running = False

    @property
    def ip_address(self):
        cands = set(self.ip_addresses())
        assert len(cands) == 1
        return cands.pop()

    def ip_addresses(self):
        res = []
        for ip_addr in self.interfaces.values():
            if isinstance(ip_addr, list):
                tmp = ip_addr
            else:
                tmp = [ip_addr]
            for t in tmp:
                if t != "":
                    res.append(t)
        return res


class SCADADevice(HostDevice, abc.ABC):
    def __init__(self, node_id, interfaces, datapoints: pandas.DataFrame):
        super().__init__(node_id, interfaces)
        self.datapoints = datapoints
        self._export_columns: typing.List[str] = []
        assert len(self.interfaces) > 0

    def to_dict(self):
        d = super().to_dict()
        d["datapoints"] = self.datapoints
        d["type"] = "scada_device"
        return d


class MTUDevice(SCADADevice):
    """
    Bundle data required for one MTU entity (formerly called "SCADA").
    """
    def __init__(self, node_id, interfaces, datapoints,
                 rtu_ips: typing.Dict[int, str]):
        super().__init__(node_id, interfaces, datapoints)
        self.rtu_ips = rtu_ips
        # rename the value column
        self.datapoints.rename(columns={"old_value": "value"}, inplace=True)
        self._export_columns = \
            ["coa", "ip", "ioa", "TK", "COT", "pp_table", "pp_column",
             "pp_index", "value"]
        self.configure()

    def configure(self):
        ips = []
        for coa in list(self.datapoints["coa"]):
            ips.append(self.rtu_ips[int(coa)])
        self.datapoints['ip'] = ips

    def to_dict(self):
        d = super().to_dict()
        d["rtu_ips"] = self.rtu_ips
        d["type"] = "mtu"
        return d


class RTUDevice(SCADADevice):
    """
    Bundle data required for one RTU entity.
    """
    def __init__(self, node_id, interfaces,
                 datapoints):
        super().__init__(node_id, interfaces, datapoints)
        self._export_columns = ["coa", "ioa", "TK", "COT", "pp_table",
                                "pp_column", "pp_index"]


    def to_dict(self):
        d = super().to_dict()
        d["type"] = "rtu"
        return d


class RouterDevice(Device):
    def __init__(self, node_id: str, interfaces: typing.Dict[str, str]):
        super().__init__(node_id, interfaces)

    def to_dict(self):
        d = super().to_dict()
        d["type"] = "router"
        return d
