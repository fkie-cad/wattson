import threading
from abc import ABC, abstractmethod
from typing import Optional, TYPE_CHECKING, Dict, final, List, Callable

from wattson.iec104.common import ConnectionState, GLOBAL_COA, MTU_DEFAULT_CONNECTION_WAIT_S
from wattson.iec104.interface.types import COT, IEC_SINGLE_VALUE, TypeID
from wattson.util import get_logger

if TYPE_CHECKING:
    from wattson.hosts.mtu.mtu import MTU


class IECClientInterface(ABC):
    def __init__(self, **kwargs):
        """
        TODO

        Args:
            **kwargs:
                
        """
        self._mtu = kwargs.get("mtu")
        self._node_id = kwargs.get("node_id", "iec-client")
        self.datapoints = kwargs.get("datapoints", [])
        log_contexts = kwargs.get("additional_contexts", set())
        self.logger = kwargs.get("logger", None)
        if self.logger is None:
            self.logger = get_logger("Wattson", "Wattson")
        self.logger = self.logger.getChild(f"{__name__.split('.')[-1]}-{str(self._node_id)}")
        self.connected_event = threading.Event()
        self.init_callbacks(self, **kwargs)

    @staticmethod
    @final
    def init_callbacks(client, **kwargs):
        """
        TODO:

        Args:
            client:
                
            **kwargs:
                
        """
        # also to be used by SingleConMaster, therefor not
        client.callbacks = {
            "on_receive_datapoint": None,
            "on_explicit_control_exit": None,
            "on_send_apdu": None,
            "on_send_raw": None,
            "on_receive_apdu": None,
            "on_receive_raw": None,
            "on_connection_change": None,
            "on_new_point": None,
            "on_receive_control_callback": None,
        }
        client.callbacks.update({
            c_name: c for c_name, c in kwargs.items() if c_name in client.callbacks
        })
        req_callbacks = ('on_send_apdu', 'on_receive_apdu')
        for c_name in req_callbacks:
            if client.callbacks[c_name] is None:
                raise ValueError(f"bad callback-init {kwargs}")

    @abstractmethod
    def add_server(self, ip: str, coa: int, **kwargs):
        """
        TODO

        Args:
            ip (str):
                
            coa (int):
                
            **kwargs:
                
        """
        ...

    @abstractmethod
    def has_server(self, coa: int) -> bool:
        ...

    @abstractmethod
    def get_servers(self) -> List:
        ...

    @abstractmethod
    def has_datapoint(self, coa: int, ioa: int) -> bool:
        ...

    @abstractmethod
    def update_datapoint(self, coa: int, ioa: int, value: IEC_SINGLE_VALUE) -> None:
        ...

    @abstractmethod
    def get_datapoint(self, coa: int, ioa: int, as_dict: bool = False):
        ...

    @abstractmethod
    def send(self, coa: int, ioa: int, cot: COT) -> bool:
        ...

    @abstractmethod
    def get_wattson_connection_state(self, coa: int) -> ConnectionState:
        ...

    def wait_for_connection(self, timeout: float = MTU_DEFAULT_CONNECTION_WAIT_S):
        return self.connected_event.wait(timeout)

    @abstractmethod
    def get_server_IP(self, coa: int) -> str:
        ...

    @abstractmethod
    def get_server_port(self, coa: int) -> int:
        ...

    @abstractmethod
    def start(self):
        ...

    @abstractmethod
    def stop(self):
        ...

    def send_sys_info_non_read(self, type_ID: int, coa: int) -> bool:
        sub_handlers: Dict[int, Callable] = {
            TypeID.C_IC_NA_1: self.send_C_IC,
            TypeID.C_CI_NA_1: self.send_C_CI,
            TypeID.C_CS_NA_1: self.send_C_CS,
            TypeID.C_RP_NA_1: self.send_C_RP
        }
        if type_ID not in sub_handlers:
            raise ValueError(f"Invalid type for sys-info control message {type_ID}")
        if coa != GLOBAL_COA and not self.has_server(coa):
            return False
        return sub_handlers[type_ID](coa)

    @abstractmethod
    def send_C_IC(self, coa: int) -> bool:
        ...

    @abstractmethod
    def send_C_CI(self, coa: int) -> bool:
        ...

    @abstractmethod
    def send_C_RP(self, coa: int) -> bool:
        ...

    @abstractmethod
    def send_C_CS(self, coa: int) -> bool:
        """
        

        Args:
            coa (int):
                either RTU-coa for single C_CS or GLOBAL-COA to send to all RTUs with GLOBAL-COA
        """
        ...

    @abstractmethod
    def send_P_AC(self, coa: int, ioa: int, cot: int, qpa: int = 3) -> Dict[str, str]:
        """
        For QPA values see 7.2.6.25 in 60870-5-101;

        Args:
            coa (int):
                
            ioa (int):
                0 to activate loaded params, != 0 to load+activate/ change cyclic tranmissions
            cot (int):
                ACT/DEACT
            qpa (int, optional):
                3 is used to activate/ deactivate cyclic transmission params
                (Default value = 3)
        """
        ...

    def send_P_ME(self, type_id: int, coa: int, ioa: int, val):
        """
        Always send with cot == ACT

        Args:
            type_id (int):
                110 - 112
            coa (int):
                
            ioa (int):
                
            val:
                
        """
        raise NotImplementedError()

