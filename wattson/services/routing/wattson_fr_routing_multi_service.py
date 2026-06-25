import time
from typing import List, TYPE_CHECKING, Optional, Callable

from wattson.services.routing.wattson_fr_mgmt_service import WattsonFrMgmtService
from wattson.services.routing.wattson_ospf_service import WattsonOSPFService
from wattson.services.routing.wattson_zebra_service import WattsonZebraService
from wattson.services.wattson_multi_service import WattsonMultiService
from wattson.services.configuration import ServiceConfiguration

if TYPE_CHECKING:
    from wattson.cosimulation.simulators.network.components.wattson_network_node import WattsonNetworkNode


class WattsonFrRoutingMultiService(WattsonMultiService):
    def __init__(self, service_configuration: 'ServiceConfiguration', network_node: 'WattsonNetworkNode'):
        self.fr_mgmt_service = WattsonFrMgmtService(service_configuration, network_node)
        services = [
            self.fr_mgmt_service,
            WattsonZebraService(ServiceConfiguration(), network_node=network_node),
            WattsonOSPFService(ServiceConfiguration(), network_node=network_node)
        ]
        service_configuration["max_wait"] = 1
        super().__init__(service_configuration, network_node, services)
        # self.start_wait = 1

    def _clean_folders(self, create: bool = True):
        # Clear folder(s)
        self.network_node.logger.debug(f"Cleaning up frr temporary folders")
        frr_namespace = self.network_node.system_name
        frr_path = f"/var/run/{frr_namespace}"
        self.network_node.exec(["rm", "-rf", frr_path])
        if create:
            self.network_node.logger.debug(f"Creating frr temporary folders")
            self.network_node.exec(["mkdir", "-p", frr_path])
            self.network_node.exec(["chown", "root:root", frr_path])
            self.network_node.exec(["chmod", "777", frr_path])

    def stop(self, wait_seconds: float = 5, auto_kill: bool = False, async_callback: Optional[Callable[['WattsonServiceInterface'], None]] = None) -> bool:
        success = super().stop(wait_seconds, auto_kill, async_callback)
        self._clean_folders(create=False)
        return success

    def start(self, refresh_config: bool = False):
        self._clean_folders()
        # App armor does stuff...
        self.network_node.exec(["aa-disable", "mgmtd"])
        self.network_node.exec(["aa-disable", "zebra"])
        self.network_node.exec(["aa-disable", "ospfd"])
        super().start(refresh_config)
        return
        # time.sleep(1)
        code, out = self.network_node.exec([
            "vtysh",
            "-N", str(self.network_node.system_name),
            "-f", str(self.fr_mgmt_service.get_tmp_config_path().absolute())],
            shell=True)
        zebra_ok: bool = False
        ospf_ok: bool = False
        for line in out:
            if "ospfd] done" in line:
                ospf_ok = True
            if "zebra] done" in line:
                zebra_ok = True
        if code == 0:
            self.network_node.logger.debug(f"Started FRRouting services. {'\n'.join(out)}")
            if not ospf_ok:
                self.network_node.logger.warning(f"OSPF configuration not applied")
            if not zebra_ok:
                self.network_node.logger.warning(f"Zebra configuration not applied")
        else:
            self.network_node.logger.warning(f"FRRouting services error: {'\n'.join(out)}")


