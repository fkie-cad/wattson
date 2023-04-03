SYSTEM_NAME = "Wattson"

MANAGEMENT_SWITCH = "s0mgm"
CLI_HOST = "wtsncli"

DEFAULT_NAMESPACE = "main"
DEFAULT_MIRROR_PORT = "tap-mr"

STAT_SERVER_ID = "statsrv"

LOG_FOLDER = "wattson_logs"
CONFIG_FOLDER = "wattson_config_tmp"

DEFAULT_HOST_DEPLOY_PRIORITY = 5

DEFAULT_LINK_DELAY = "0ms"
DEFAULT_LINK_JITTER = None
DEFAULT_LINK_DATARATE = "1Gbps"
DEFAULT_LINK_LOSS = 0
DEFAULT_MTU_DEPLOY = {
    "type": "python",
    "module": "wattson.hosts.mtu",
    "class": "MtuDeployment",
    "config": {
        "datapoints": "!datapoints.!nodeid",
        "rtus": "!rtu_map.!nodeid",
        "coordinator_ip": "!primary_ips.coord",
        "coordinator_mgm": "!management_ips.coord",
        "nodeid": "!nodeid",
        "coa": "!coa",
        "ip": "!ip",
        "connect_delay": "!globals.mtu_connect_delay",
        "do_general_interrogation": "!globals.do_general_interrogation",
        "do_clock_sync": "!globals.do_clock_sync",
        "statistics": "!globals.statistics",
        "scenario_path": "!scenario_path",
    }
}
DEFAULT_RTU_DEPLOY = {
    "type": "python",
    "module": "wattson.hosts.rtu",
    "class": "RtuDeployment",
    "config": {
        "datapoints": "!datapoints.!nodeid",
        "coordinator_ip": "!primary_ips.coord",
        "coordinator_mgm": "!management_ips.coord",
        "nodeid": "!nodeid",
        "coa": "!coa",
        "ip": "!ip",
        "periodic_update_ms": "!globals.periodic_update_ms",
        "fields": "!fields.!nodeid",
        "periodic_update_start": "!globals.periodic_update_start",
        "do_periodic_updates": "!globals.do_periodic_updates",
        "rtu_logic": "!globals.rtu_logic",
        "statistics": "!globals.statistics",
        "powernet": "!raw_powernet",
        "scenario_path": "!scenario_path",
    }
}
DEFAULT_FIELD_DEPLOY = {
    "type": "python",
    "module": "wattson.hosts.field",
    "class": "FieldDeployment",
    "config": {
        "datapoints": "!datapoints.!nodeid",
        "coordinator_ip": "!primary_ips.coord",
        "coordinator_mgm": "!management_ips.coord",
        "nodeid": "!nodeid",
        "ip": "!ip",
        "statistics": "!globals.statistics"
    }
}

STAT_SERVER_DEPLOY = {
    "type": "python",
    "module": "wattson.analysis.statistics.server.deployment",
    "class": "StatisticServerDeployment",
    "config": {
        "nodeid": "!nodeid",
        "ip": "!ip",
        "statistics": "!globals.statistics",
        "datapoints": "!datapoints",
        "network": "!network_graph",
        "powernet": "!raw_powernet"
    }
}

STAT_SERVER_NODE = {
    "id": STAT_SERVER_ID,
    "name": "Statistic Server",
    "type": "host",
    "shutdown_wait": 30,
    "priority": 2,
    "interfaces": [],
    "deploy": STAT_SERVER_DEPLOY
}
