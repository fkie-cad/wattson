from abc import ABC
from dataclasses import dataclass
import json
from typing import Dict, Union, Tuple, Optional, Any, Iterable
import inspect
from enum import IntEnum, unique, Enum
from copy import deepcopy


from wattson.apps.interface.util import ConfirmationStatus, FailReason
from wattson.apps.interface.util.constants import (
    UNSET_REFERENCE_NR,
    WITHOUT_COMMAND,
    DEFAULT_MAX_TRIES
)

from wattson.iec104.common import ConnectionState
from wattson.iec104.common.config import SUPPORTED_ASDU_TYPES
from wattson.iec104.interface.types import MsgDirection, TypeID, COT, IEC_PARAMETER_SINGLE_VALUE
from wattson.iec104.interface.apdus import I_FORMAT, APDU

COA = Union[int, str]
IOA = Union[int, str]


@unique
class MsgID(IntEnum):
    PROCESS_INFO_MONITORING = 1
    PROCESS_INFO_CONTROL = 2
    PARAMETER_ACTIVATE = 3
    PARAMETER_LOAD = 4
    SYS_INFO_CONTROL = 5
    SYS_INFO_MONITORING = 6
    FILE_TRANSFER_REQ = 7
    FILE_TRANSFER_REPLY = 8
    CONF = 9
    # GENERAL_INTERRO_FIN = 10
    TOTAL_INTERRO_REQ = 11
    TOTAL_INTERRO_REP = 12
    RTU_STATUS_REQ = 13
    RTU_STATUS_REPLY = 14
    # GENERAL_INTERRO_START = 15
    READ_DATAPOINT = 16
    PERIODIC_UPDATE = 17
    #
    CONNECTION_STATUS_CHANGE = 18
    DISCONNECT_CANCEL_MSGS = 21
    # MTU Cache
    MTU_CACHE_REQ = 19
    MTU_CACHE_REPLY = 20

    def to_class(self):
        return {
            # MsgID.GENERAL_INTERRO_FIN: GenInterroFin,
            MsgID.PROCESS_INFO_MONITORING: ProcessInfoMonitoring,
            MsgID.PROCESS_INFO_CONTROL: ProcessInfoControl,
            MsgID.CONF: Confirmation,
            MsgID.PARAMETER_ACTIVATE: ParameterActivate,
            MsgID.SYS_INFO_CONTROL: SysInfoControl,
            MsgID.FILE_TRANSFER_REPLY: FileTransferReply,
            MsgID.FILE_TRANSFER_REQ: FileTransferReq,
            MsgID.TOTAL_INTERRO_REQ: TotalInterroReq,
            MsgID.TOTAL_INTERRO_REP: TotalInterroReply,
            MsgID.RTU_STATUS_REQ: RTUStatusReq,
            MsgID.RTU_STATUS_REPLY: RTUStatusReply,
            MsgID.READ_DATAPOINT: ReadDatapoint,
            MsgID.PERIODIC_UPDATE: PeriodicUpdate,
            MsgID.CONNECTION_STATUS_CHANGE: ConnectionStatusChange,
            MsgID.DISCONNECT_CANCEL_MSGS: DisconnectCancelMsgsChange,
            MsgID.MTU_CACHE_REQ: MtuCacheReq,
            MsgID.MTU_CACHE_REPLY: MtuCacheReply
        }[self]

    @property
    def direction(self):
        return {
            MsgID.PROCESS_INFO_MONITORING: MsgDirection.MONITORING,
            MsgID.PROCESS_INFO_CONTROL: MsgDirection.CONTROL,
            MsgID.PARAMETER_ACTIVATE: MsgDirection.CONTROL,
            MsgID.SYS_INFO_CONTROL: MsgDirection.CONTROL,
            MsgID.SYS_INFO_MONITORING: MsgDirection.MONITORING,
            MsgID.FILE_TRANSFER_REQ: MsgDirection.CONTROL,
            MsgID.FILE_TRANSFER_REPLY: MsgDirection.CONTROL,
            MsgID.READ_DATAPOINT: MsgDirection.CONTROL,
            MsgID.PERIODIC_UPDATE: MsgDirection.MONITORING,
            MsgID.TOTAL_INTERRO_REQ: MsgDirection.CONTROL,
            MsgID.TOTAL_INTERRO_REP: MsgDirection.APPLICATION_REPLY,
            MsgID.RTU_STATUS_REQ: MsgDirection.CONTROL,
            MsgID.RTU_STATUS_REPLY: MsgDirection.APPLICATION_REPLY,
            MsgID.MTU_CACHE_REQ: MsgDirection.CONTROL,
            MsgID.MTU_CACHE_REPLY: MsgDirection.APPLICATION_REPLY,
            MsgID.CONNECTION_STATUS_CHANGE: MsgDirection.APPLICATION_REPLY,
            MsgID.DISCONNECT_CANCEL_MSGS: MsgDirection.APPLICATION_REPLY,
        }[self]

    @property
    def iec_layer(self):
        return self not in {
            MsgID.RTU_STATUS_REQ,
            MsgID.RTU_STATUS_REPLY,
            MsgID.TOTAL_INTERRO_REQ,
            MsgID.TOTAL_INTERRO_REP,
        }

    @staticmethod
    def from_type(type_ID: int, cot: int = 0) -> "MsgID":
        if 1 <= type_ID <= 21 or 30 <= type_ID <= 40:
            if cot == COT.PERIODIC:
                return MsgID.PERIODIC_UPDATE
            return MsgID.PROCESS_INFO_MONITORING
        if 45 <= type_ID <= 51 or 58 <= type_ID <= 64:
            return MsgID.PROCESS_INFO_CONTROL
        if type_ID == 70:
            return MsgID.SYS_INFO_MONITORING
        if type_ID == TypeID.C_RD_NA_1:
            return MsgID.READ_DATAPOINT
        if 100 <= type_ID <= 107:
            return MsgID.SYS_INFO_CONTROL
        if 110 <= type_ID <= 112:
            return MsgID.PARAMETER_LOAD
        if type_ID == TypeID.P_AC_NA_1:
            return MsgID.PARAMETER_ACTIVATE
        # TODO: check which filetransfer ones are exactly request/ reply.
        # but not used anyway for now
        if type_ID in {122, 127}:
            return MsgID.FILE_TRANSFER_REQ
        if type_ID in {120, 121, 123, 124, 125, 126}:
            return MsgID.FILE_TRANSFER_REPLY


class SubscriptionInitMsg:
    def __init__(self, subscriber_type: str, **kwargs):
        self.subscriber_type = subscriber_type
        self.other_params = kwargs if kwargs else {}
        if "subscriber_type" in kwargs:
            del kwargs["subscriber_type"]

    def __str__(self):
        return f'{self.__class__.__name__} with type: {self.subscriber_type} and params: {self.other_params}'

    def to_json(self) -> str:
        self_dict = deepcopy(self.other_params)
        self_dict["subscriber_type"] = self.subscriber_type
        return json.dumps(self_dict)


class SubscriptionInitReply:
    # give subscriber prefix-ID in reply to ensure no collision between subscribers
    def __init__(self, subscriber_ID: str, **kwargs):
        self.subscriber_ID = subscriber_ID
        self.other_params = kwargs if kwargs else {}
        if "subscriber_ID" in kwargs:
            del kwargs["subscriber_ID"]

    def __str__(self):
        return f'{self.__class__.__name__} with ID {self.subscriber_ID} and params {self.other_params}'

    def to_json(self) -> str:
        self_dict = deepcopy(self.other_params)
        self_dict["subscriber_ID"] = self.subscriber_ID
        return json.dumps(self_dict)


@dataclass
class ConnectionStatusChange:
    coa: int
    connected: bool
    ip: str
    port: int
    reference_nr: str

    def __post_init__(self):
        self.id = MsgID.CONNECTION_STATUS_CHANGE

    def to_json(self):
        return json.dumps(self.__dict__)

    def __str__(self):
        return str(self.__dict__)


@dataclass
class DisconnectCancelMsgsChange:
    """
    Notifies subscribers that the resp. RTU has gone down and lists all msgs
    which have been cancelled/ for which no further update will be send.
    """
    coa: int
    ip: str
    port: int
    reference_nr: str
    cancelled_ref_nrs: Iterable[str]

    def __post_init__(self):
        self.id = MsgID.DISCONNECT_CANCEL_MSGS

    def to_json(self):
        return json.dumps(self.__dict__)
# maybe make a message for "MTU command" that is not forwarded to RTUs
# and handles REQ-REPLY better than through two separate IDs


class IECMsg(ABC):
    id: MsgID
    reference_nr: str
    max_tries: int

    def to_json(self) -> str:
        return json.dumps(self.__dict__)

    def __post_init__(self):
        self.id = _msg_class_id_resolver[self.__class__]
        if (
            self.id in {MsgID.PROCESS_INFO_MONITORING, MsgID.PROCESS_INFO_CONTROL}
            and self.type_ID not in SUPPORTED_ASDU_TYPES
        ):
            raise NotImplementedError(f"Message for typeID {self.type_ID} is unsupported")
        if not self.reference_nr:
            self.reference_nr = UNSET_REFERENCE_NR

        self.type = self._get_type()

        self_dict = self.__dict__
        if "cot" in self_dict:
            self.cot = COT(int(self.cot))
        if "type_ID" in self_dict:
            self.type_ID = TypeID(int(self.type_ID))
        if "coa" in self_dict and not isinstance(self.coa, int):
            raise ValueError("coa has to be of type int")
        self.update_stringified_keys()

    def _get_type(self):
        return self.__class__.__name__

    def update_stringified_keys(self):
        self_dict = self.__dict__
        if "val_map" in self_dict:
            new_map = {}
            for ioa, val in self.val_map.items():
                new_map[int(ioa)] = TypeID.convert_val_by_type(self.type_ID, val)

            new_map = {int(ioa): TypeID.convert_val_by_type(self.type_ID, val) for ioa, val in self.val_map.items()}
            self.val_map = new_map

        if "ts_map" in self_dict:
            new_map = {int(ioa): ts for ioa, ts in self.ts_map.items()}
            self.ts_map = new_map

        if "status" in self.__dict__:
            new_status = {int(key): val for key, val in self.status.items()}
            self.status = new_status

        if "datapoints" in self.__dict__:
            new_dps = {int(coa): {int(ioa): dp for ioa, dp in _map.items()} for coa, _map in self.datapoints.items()}
            self.datapoints = new_dps

    @staticmethod
    def from_apdu(
            apdu: APDU, new_reference_nr: Optional[str] = None,
            send: bool = False,
    ) -> 'IECMsg':
        if isinstance(apdu, I_FORMAT):
            msg_id = MsgID.from_type(apdu.type)
            max_tries = 1 - int(send)
            reference_nr = new_reference_nr if new_reference_nr else WITHOUT_COMMAND
            if msg_id == MsgID.SYS_INFO_CONTROL:
                return SysInfoControl(apdu.type, apdu.coa, apdu.cot, reference_nr, max_tries)
            elif msg_id == MsgID.PROCESS_INFO_MONITORING:
                assert apdu.ioas != [0]
                val_map = {ioa: -1 for ioa in apdu.ioas}
                ts_map = {ioa: -1 for ioa in apdu.ioas}
                # TODO - extract val somehow, potentially combine Objects
                return ProcessInfoMonitoring(apdu.coa, val_map, ts_map, apdu.type, apdu.cot, reference_nr)

            elif msg_id == MsgID.PROCESS_INFO_CONTROL:
                assert apdu.ioas != [0]
                val_map = {ioa: -1 for ioa in apdu.ioas}
                return ProcessInfoControl(
                    apdu.coa, apdu.type, val_map, reference_nr, max_tries, cot=apdu.cot
                )

            elif msg_id == MsgID.READ_DATAPOINT:
                assert apdu.ioas != [0]
                return ReadDatapoint(apdu.coa, apdu.ioas[0], reference_nr, max_tries)

            raise NotImplementedError(f"Bad msg id {msg_id}")

        raise NotImplementedError(f"Bad format {type(apdu)}")

    @property
    def values(self):
        return "val_map" in self.__dict__

    @property
    def direction(self):
        if "type_ID" in self.__dict__:
            return TypeID(self.type_ID).direction
        return self.id.direction

    @property
    def mtu_initiated(self) -> bool:
        return self.reference_nr.startswith("MTU")

    def __str__(self):
        return str(self.__dict__)

    @property
    def iec_layer(self):
        return self.id.iec_layer


@dataclass
class ProcessInfoMonitoring(IECMsg):
    """ coa = source COA (RTU) """
    # packet containing read-values send to MTU from RTUs, new_state)
    # all IOs reference to the same coa - one IEC-104 packet's COA applies to all IOs
    # value is tuple in case of timestamped readings
    # send with empty val_map and cot == 10 to mark end of packets about that typeID
    # during general interro
    coa: int
    val_map: Dict[IOA, Union[bool, int, float, Tuple]]
    ts_map: Dict[int, int]
    type_ID: Union[int, TypeID]
    cot: int
    reference_nr: str

    def __post_init__(self):
        '''
        if self.type_ID in TypeID._value2member_map_:
            for ioa in self.val_map:
                self.val_map[ioa] = TypeID.convert_val_by_type(self.type_ID, self.val_map[ioa])
        '''
        self.max_tries = 0  # in monitory direction, there is no point of it really
        super().__post_init__()


@dataclass
class SysInfoMonitoring(IECMsg):
    """ coa = RTU's COA """
    coa: int
    coi: int = -1

    def __post_init__(self):
        self.id: MsgID = MsgID.SYS_INFO_MONITORING
        self.type_ID = 70
        self.max_tries = 0


@dataclass
class ProcessInfoControl(IECMsg):
    # write-command (typle-45-69) to be send from MTU to RTU
    # all IOs reference to the same coa - one IEC-104 packet's COA applies to all IOs
    # COT necessary if the MTU sends a set-command not communicated over a subscriber
    coa: COA
    type_ID: int
    val_map: Dict[IOA, Union[bool, int, float]]
    reference_nr: str = UNSET_REFERENCE_NR
    max_tries: int = DEFAULT_MAX_TRIES
    queue_on_collision: bool = False
    cot: int = COT.ACTIVATION
    qualifier: int = 0
    #quality: QualityByte = field(default_factory=QualityByte)
    _raw_io = None
    select_execute: bool = False


@dataclass
class Confirmation(IECMsg):
    """
        message informing a command was (not) successful
        result is dictionary with following fixed key-val pairs:
            - 'status' ( -> Failed, Successful X, ...)
            - 'reason' (if result['status'] == 'Failed')
        Depending on the original message, other values can be added
            to the result.
        max tries = orig_msg.max_tries - transmission_tries_until_success_or_0
    """
    result: Dict[str, Union[bool, int, str, ConfirmationStatus, FailReason]]
    reference_nr: str
    max_tries: int  # from orig msg, decremented by 1 for each failure

    def to_json(self) -> str:
        d = self.__dict__
        if 'reason' in self.result:
            if isinstance(self.result["reason"], FailReason):
                d['result']['reason'] = self.result['reason'].value
            elif isinstance(self.result["reason"], str):
                d['result']['reason'] = self.result['reason']
        if not isinstance(self.result['status'], str):
            d['result']['status'] = self.result['status'].value

        return json.dumps(d)

    @staticmethod
    def from_result_and_previous_msg(result: dict, prev_msg: IECMsg) -> 'Confirmation':
        return Confirmation(result, prev_msg.reference_nr, prev_msg.max_tries)


@dataclass
class ParameterActivate(IECMsg):
    """
    activate: act/deact loaded params/ cyclic periodic updates

    ioa == 0 -> perform act/deact on loaded params
    ioa != 0 -> act/deact cyclic updates for that ioa
        cyclic changes will fail for actuator IOAs
            (typeID in [45, 64])
    """
    coa: int
    ioa: int
    activate: bool
    reference_nr: str = UNSET_REFERENCE_NR
    max_tries: int = DEFAULT_MAX_TRIES
    queue_on_collision: bool = False


@dataclass
class ParameterLoad(IECMsg):
    """
    Loads new parameters, e.g., thresholds, for later synchronised activation.
    """
    coa: int
    ioa: int
    value: IEC_PARAMETER_SINGLE_VALUE
    reference_nr: str = UNSET_REFERENCE_NR
    max_tries: int = DEFAULT_MAX_TRIES
    queue_on_collision: bool = False


class SysInfoControl(IECMsg):
    # general system commands, for now always considered global
    # TODO: need to think how to handle the val of e.g., time-synch command
    def __init__(
        self,
        type_ID: int,
        coa: int,
        cot: int,
        reference_nr: str = UNSET_REFERENCE_NR,
        max_tries: int = DEFAULT_MAX_TRIES,
        queue_on_collision: bool = False,
        **kwargs,
    ):
        """ coa = RTU's COA """
        self.id = MsgID.SYS_INFO_CONTROL
        self.type_ID = type_ID
        self.coa = coa
        self.cot = COT(int(cot))
        self.max_tries = max_tries
        self.queue_on_collision = queue_on_collision
        self.reference_nr = reference_nr
        if "ioa" in kwargs:
            self.ioa = kwargs["ioa"]
        if "value" in kwargs:
            self.value = kwargs["value"]


class FileTransferReq(IECMsg):
    def __init__(self, reference_nr: str = UNSET_REFERENCE_NR,
                 queue_on_collision: bool = False):
        self.id = MsgID.FILE_TRANSFER_REQ
        self.reference_nr = reference_nr
        self.queue_on_collision = queue_on_collision
        raise NotImplementedError()

    # not used by wattson yet


class FileTransferReply(IECMsg):
    def __init__(self, reference_nr: str):
        self.id = MsgID.FILE_TRANSFER_REPLY
        self.reference_nr = reference_nr
        raise NotImplementedError()


@dataclass
class TotalInterroReq(IECMsg):
    reference_nr: str = UNSET_REFERENCE_NR


@dataclass
class TotalInterroReply(IECMsg):
    # forwards all datapoints, RTUs, etc. known to the MTU; currently only iec-datapoints + RTU Conn-status
    # named tuple are simply converted into json list like regular tuples
    status: Dict[int, Tuple[int, str, int]]
    datapoints: Dict[int, Dict[int, Tuple[int, int, int, int, str, str]]]
    reference_nr: str


@dataclass
class RTUStatusReply(IECMsg):
    # forwards connections status to all RTUs
    status: Dict[int, Tuple[int, str, ConnectionState]]
    reference_nr: str


@dataclass
class RTUStatusReq(IECMsg):
    # request RTU status update
    reference_nr: str = UNSET_REFERENCE_NR


@dataclass
class MtuCacheReply(IECMsg):
    # Forwards MTU cache containing latest values for all data points
    cache: Dict[str, Dict[str, Any]]
    reference_nr: str


@dataclass
class MtuCacheReq(IECMsg):
    # Requests MTU cache state
    reference_nr: str = UNSET_REFERENCE_NR


@dataclass
class ReadDatapoint(IECMsg):
    """
    Special case to request reading a datapoint
    TODO: Currently only supports single datapoint-reads
    """
    coa: int
    ioa: int
    reference_nr: str = UNSET_REFERENCE_NR
    max_tries: int = DEFAULT_MAX_TRIES
    queue_on_collision: bool = False
    qualifier: int = 0
    _raw_io = None
    select_execute: bool = False

    def __post_init__(self):
        self.type_ID = TypeID.C_RD_NA_1
        self.cot = COT.INTERROGATION
        super().__post_init__()


@dataclass(init=False)
class PeriodicUpdate(ProcessInfoMonitoring):
    """ Multiple IOs from periodic update, currently not forwarded """
    def __init__(
            self,
            coa: int,
            val_map: Dict[IOA, Union[bool, int, float, Tuple]],
            ts_map: Dict[int, int],
            type_ID: Union[int, TypeID],
            reference_nr: str,
    ):
        super().__init__(coa, val_map, ts_map, type_ID, COT.PERIODIC, reference_nr)


def from_json(serialised_json) -> Union[IECMsg, SubscriptionInitReply, SubscriptionInitMsg]:
    if not isinstance(serialised_json, dict):
        try:
            msg_dict = json.loads(serialised_json)
        except json.JSONDecodeError as e:
            print("JSON error" + str(serialised_json))
            raise e
    else:
        msg_dict = serialised_json

    return from_dict(msg_dict)


def from_dict(msg_dict) -> Union[IECMsg, SubscriptionInitReply, SubscriptionInitMsg]:
    if "subscriber_type" in msg_dict:
        return SubscriptionInitMsg(msg_dict["subscriber_type"], kwargs=msg_dict)
    elif "subscriber_ID" in msg_dict:
        return SubscriptionInitReply(msg_dict["subscriber_ID"], kwargs=msg_dict)

    if "id" not in msg_dict:
        raise ValueError(f"Received invalid message {msg_dict}")
    update_val_map(msg_dict)

    _class = MsgID(msg_dict["id"]).to_class()
    sig = inspect.signature(_class.__init__)
    _args = []
    _kwargs = {}
    for param in sig.parameters.values():
        if "self" != param.name != "kwargs":
            try:
                if isinstance(param.default, Enum):
                    _def_val = param.default.value
                else:
                    _def_val = param.default
                if inspect.Parameter.empty == _def_val:
                    _args.append(msg_dict[param.name])
                else:
                    _kwargs[param.name] = msg_dict[param.name]
            except TypeError as e:
                print(e)
                print(f'{param=} {inspect=}', flush=True)
                raise e
        elif param.name == "kwargs":
            for key, val in msg_dict.items():
                if val not in _args and key not in _kwargs:
                    _kwargs[key] = val

    # currently assumes no
    if _kwargs:
        msg = _class(*(tuple(_args)), **(_kwargs))
    else:
        msg = _class(*(tuple(_args)))
    return msg


def update_val_map(msg_dict: Dict):
    """ parses stringified IOAs in the json dict to ints """
    if "val_map" in msg_dict:
        msg_dict["val_map"] = {int(ioa): val for (ioa, val) in msg_dict["val_map"].items()}
    if "ioa" in msg_dict:
        msg_dict["ioa"] = int(msg_dict["ioa"])


_msg_class_id_resolver = {
        ProcessInfoMonitoring: MsgID.PROCESS_INFO_MONITORING,
        SysInfoMonitoring: MsgID.SYS_INFO_MONITORING,
        ProcessInfoControl: MsgID.PROCESS_INFO_CONTROL,
        Confirmation: MsgID.CONF,
        ParameterActivate: MsgID.PARAMETER_ACTIVATE,
        ParameterLoad: MsgID.PARAMETER_LOAD,
        SysInfoControl: MsgID.SYS_INFO_CONTROL,
        FileTransferReq: MsgID.FILE_TRANSFER_REQ,
        FileTransferReply: MsgID.FILE_TRANSFER_REPLY,
        TotalInterroReq: MsgID.TOTAL_INTERRO_REQ,
        TotalInterroReply: MsgID.TOTAL_INTERRO_REP,
        RTUStatusReply: MsgID.RTU_STATUS_REPLY,
        RTUStatusReq: MsgID.RTU_STATUS_REQ,
        ReadDatapoint: MsgID.READ_DATAPOINT,
        PeriodicUpdate: MsgID.PERIODIC_UPDATE,
        ConnectionStatusChange: MsgID.CONNECTION_STATUS_CHANGE,
        MtuCacheReq: MsgID.MTU_CACHE_REQ,
        MtuCacheReply: MsgID.MTU_CACHE_REPLY
    }

TO_MTU_MSG_TYPES = Union[
    RTUStatusReq,
    MtuCacheReq,
    SubscriptionInitMsg,
    ProcessInfoControl,
    ParameterActivate,
    ParameterLoad,
    SysInfoControl,
    FileTransferReq,
    TotalInterroReq,
    ReadDatapoint,
]

FROM_MTU_MSG_TYPES = Union[
    Confirmation,
    RTUStatusReply,
    MtuCacheReply,
    SubscriptionInitReply,
    ConnectionStatusChange,
    DisconnectCancelMsgsChange,
    # not exactly all IECMsgs, but almost all can be send by the MTU...
    # TODO: we don't have a clear type-wise distinction between msgs directly relating to the traffic
    # and indirect control msgs - e.g. RTUStatusReq is subtype of IECMsg, although it will never
    # touch the actual traffic
    IECMsg
]
