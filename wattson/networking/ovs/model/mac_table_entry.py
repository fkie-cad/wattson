class MacTableEntry:
    def __init__(self, age: int, mac_address: str, port: int, port_name: str, vlan: int):
        self.age = age
        self.mac_address = mac_address
        self.port = port
        self.port_name = port_name
        self.vlan = vlan

    def to_dict(self):
        return {
            "age": self.age,
            "mac_address": self.mac_address,
            "port": self.port,
            "port_name": self.port_name,
            "vlan": self.vlan
        }
