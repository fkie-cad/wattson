from wattson.services.configuration import ServiceConfiguration


class MtuDefaultConfiguration(ServiceConfiguration):
    def __init__(self):
        super().__init__()
        self.update({
            "datapoints": "!datapoints.!nodeid",
            "name": "MTU/104-Client",
            "rtus": "!rtu_map.!entityid",
            "wattson_client_config": {
                "query_socket": "!sim-control-query-socket",
                "publish_socket": "!sim-control-publish-socket",
            },
            "nodeid": "!nodeid",
            "entityid": "!entityid",
            "ip": "!ip",
            "connect_delay": "!mtu_connect_delay",
            "do_general_interrogation": "!do_general_interrogation",
            "do_clock_sync": "!do_clock_sync",
            "statistics": "!statistics",
            "scenario_path": "!scenario_path",
            "mtu_logic": "!mtu_logic"
        })
        self.priority.set_local(2)
