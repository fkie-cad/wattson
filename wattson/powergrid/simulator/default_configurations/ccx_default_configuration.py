from wattson.services.configuration import ServiceConfiguration


class CCXDefaultConfiguration(ServiceConfiguration):
    def __init__(self):
        super().__init__()
        self.update({
            "node_id": "!nodeid",
            "data_points": "!datapoints.!nodeid",
            "name": "WattsonCCX",
            "servers": "!rtu_map.!entityid",    # TODO: Adjust for multiple protocols
            "wattson_client_config": {
                "query_socket": "!sim-control-query-socket",
                "publish_socket": "!sim-control-publish-socket",
            },
            "ip": "!ip",
            "connect_delay": "!mtu_connect_delay",
            "iec104": {
                "do_general_interrogation": "!do_general_interrogation",
                "do_clock_sync": "!do_clock_sync",
            },
            "scenario_path": "!scenario_path",
            "logics": "!ccx_logic"
        })
        self.priority.set_local(2)
