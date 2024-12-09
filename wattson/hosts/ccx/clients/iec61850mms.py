from typing import TYPE_CHECKING

from wattson.hosts.ccx.clients.ccx_client import CCXProtocolClient
from wattson.hosts.ccx.protocols import CCXProtocol

if TYPE_CHECKING:
    from wattson.hosts.ccx import ControlCenterExchangeGateway


class Iec61850MMSCCXProtocolClient(CCXProtocolClient):
    def __init__(self, ccx: 'ControlCenterExchangeGateway'):
        super().__init__(ccx)
        
    def get_protocol(self) -> CCXProtocol:
        return CCXProtocol.IEC61850_MMS
