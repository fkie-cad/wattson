from wattson.services.configuration import ServiceConfiguration


class RtuDefaultConfiguration(ServiceConfiguration):
    def __init__(self):
        super().__init__()
        self.update({
            "datapoints": "!datapoints.!nodeid",
            "name": "WattsonRTU",
            "wattson_client_config": {
                "query_socket": "!sim-control-query-socket",
                "publish_socket": "!sim-control-publish-socket",
            },
            "nodeid": "!nodeid",
            "entityid": "!entityid",
            "coa": "!coa",
            "ip": "!ip",
            "periodic_update_ms": "!periodic_update_ms",
            "periodic_update_start": "!periodic_update_start",
            "do_periodic_updates": "!do_periodic_updates",
            "rtu_logic": "!rtu_logic",
            "statistics": "!statistics",
            "scenario_path": "!scenario_path",
            "allowed_mtu_ips": "!allowed_mtu_ips",
            "local_control": False
        })
