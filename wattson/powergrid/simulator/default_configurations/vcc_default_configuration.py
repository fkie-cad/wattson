from wattson.services.configuration import ServiceConfiguration


class VccDefaultConfiguration(ServiceConfiguration):
    def __init__(self):
        super().__init__()
        self.update({
            "datapoints": "!datapoints",
            "power_grid": "!power_grid_model",
            "name": "Wattson VCC",
            "wattson_client_config": {
                "query_socket": "!sim-control-query-socket",
                "publish_socket": "!sim-control-publish-socket",
            },
            "nodeid": "!nodeid",
            "entityid": "!entityid",
            "ip": "!ip",
            "mtu_ip": "!management_ips.!mtus.0",
            "scenario_path": "!scenario_path"
        })
        self.priority.set_local(0)
