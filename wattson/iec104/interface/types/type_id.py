from copy import deepcopy
from enum import unique, IntEnum
from typing import Dict, Set, Type

from wattson.iec104.interface.types.custom_iec_value import IEC_SINGLE_VALUE
from wattson.iec104.interface.types.msg_direction import MsgDirection


@unique
class TypeID(IntEnum):
    # UNDEFINED = 0     TODO: Should make this None where it is necessary
    # single-value information
    M_SP_NA_1 = 1
    M_SP_TA_1 = 2
    M_DP_NA_1 = 3
    M_DP_TA_1 = 4
    M_ST_NA_1 = 5
    M_ST_TA_1 = 6
    M_BO_NA_1 = 7
    M_BO_TA_1 = 8
    M_ME_NA_1 = 9
    M_ME_TA_1 = 10
    M_ME_NB_1 = 11
    M_ME_TB_1 = 12
    M_ME_NC_1 = 13
    M_ME_TC_1 = 14
    M_IT_NA_1 = 15
    M_IT_TA_1 = 16
    M_EP_TA_1 = 17
    M_EP_TB_1 = 18
    M_EP_TC_1 = 19
    M_PS_NA_1 = 20
    M_ME_ND_1 = 21

    # with time-tag CP56Time2a
    M_SP_TB_1 = 30
    M_DP_TB_1 = 31
    M_ST_TB_1 = 32
    M_BO_TB_1 = 33
    M_ME_TD_1 = 34
    M_ME_TE_1 = 35
    M_ME_TF_1 = 36
    M_IT_TB_1 = 37
    M_EP_TD_1 = 38
    M_EP_TE_1 = 39
    M_EP_TF_1 = 40

    # Process-Info Commands without time-tag
    C_SC_NA_1 = 45      # Single Command
    C_DC_NA_1 = 46      # Double Command
    C_RC_NA_1 = 47      # Step Command
    C_SE_NA_1 = 48
    C_SE_NB_1 = 49
    C_SE_NC_1 = 50
    C_BO_NA_1 = 51

    # Process-Info commands with long time tag
    C_SC_TA_1 = 58      # Single Command
    C_DC_TA_1 = 59      # Double Command
    C_RC_TA_1 = 60      # Step Command
    C_SE_TA_1 = 61
    C_SE_TB_1 = 62
    C_SE_TC_1 = 63
    C_BO_TA_1 = 64

    M_EI_NA_1 = 70      # End Initialization

    C_IC_NA_1 = 100     # (General-) Interrogation
    C_CI_NA_1 = 101     # Counter Interro
    C_RD_NA_1 = 102     # Read-Request
    C_CS_NA_1 = 103     # Clock Sync
    C_TS_NA_1 = 104
    C_RP_NA_1 = 105    # Reset Process5
    C_CD_NA_1 = 106
    C_TS_TA_1 = 107     # Capture Datagram Runtime

    P_ME_NA_1 = 110     # Measurement normalized
    P_ME_NB_1 = 111     # Measurement scaled
    P_ME_NC_1 = 112     # Measurement short float
    P_AC_NA_1 = 113     # Activation Parameter

    # Files
    F_FR_NA_1 = 120     # File Ready
    F_SR_NA_1 = 121     # Section Ready
    F_SC_NA_1 = 122     # Requests
    F_LS_NA_1 = 123     # last Segment
    F_AF_NA_1 = 124     # File / Segment Confirmation
    F_SG_NA_1 = 125     # Segment
    F_DR_TA_1 = 126     # File Directory
    F_SC_NB_1 = 127     # Request archive

    @property
    def invalidated_for_IEC104(self) -> bool:
        """ For supported type-table, see DIN EN 60870-5-104, Section 9.5 """
        invalid_types_for_104 = {
            TypeID.M_SP_TA_1,
            TypeID.M_DP_TA_1,
            TypeID.M_ST_TA_1,
            TypeID.M_BO_TA_1,
            TypeID.M_ME_TA_1,
            TypeID.M_ME_TB_1,
            TypeID.M_ME_TC_1,
            TypeID.M_IT_NA_1,
            TypeID.M_EP_TA_1,
            TypeID.M_EP_TB_1,
            TypeID.M_EP_TC_1,
            TypeID.C_TS_NA_1,
            TypeID.C_CD_NA_1,
        }
        return self in invalid_types_for_104

    @property
    def expects_IOA_as_0(self) -> bool:
        """
        If an APDU send with this TypeID expects the IOA to be set to 0
        This is the case when the information requested/ command does not belong to an
            IO with an IOA. (Clock-synch, global interro, ...)
        """
        types_with_IOA_as_0 = {
            TypeID.C_CS_NA_1,
            TypeID.C_IC_NA_1,
            TypeID.C_CI_NA_1,
            TypeID.C_RP_NA_1,
            TypeID.M_EI_NA_1,
        }
        return self in types_with_IOA_as_0

    @property
    def expects_single_IO(self) -> bool:
        """ If an APDU with this TypeID should only have 1 IO to be considered valid. """
        if self.expects_IOA_as_0:
            return True
        only_single_IOs = {
            TypeID.P_AC_NA_1,
        }
        return self in only_single_IOs

    @staticmethod
    def values() -> Set[int]:
        """ Defined Type-IDs as integers"""
        return deepcopy(set(TypeID._value2member_map_.keys()))

    @staticmethod
    def from_name(s):
        return TypeID.names()[s]

    @staticmethod
    def names():
        return {'M_SP_NA_1': TypeID.M_SP_NA_1, 'M_SP_TA_1': TypeID.M_SP_TA_1,
                'M_DP_NA_1': TypeID.M_DP_NA_1, 'M_DP_TA_1': TypeID.M_DP_TA_1,
                'M_ST_NA_1': TypeID.M_ST_NA_1, 'M_ST_TA_1': TypeID.M_ST_TA_1,
                'M_BO_NA_1': TypeID.M_BO_NA_1, 'M_BO_TA_1': TypeID.M_BO_TA_1,
                'M_ME_NA_1': TypeID.M_ME_NA_1, 'M_ME_TA_1': TypeID.M_ME_TA_1,
                'M_ME_NB_1': TypeID.M_ME_NB_1, 'M_ME_TB_1': TypeID.M_ME_TB_1,
                'M_ME_NC_1': TypeID.M_ME_NC_1, 'M_ME_TC_1': TypeID.M_ME_TC_1,
                'M_IT_NA_1': TypeID.M_IT_NA_1, 'M_IT_TA_1': TypeID.M_IT_TA_1,
                'M_EP_TA_1': TypeID.M_EP_TA_1, 'M_EP_TB_1': TypeID.M_EP_TB_1,
                'M_EP_TC_1': TypeID.M_EP_TC_1, 'M_PS_NA_1': TypeID.M_PS_NA_1,
                'M_ME_ND_1': TypeID.M_ME_ND_1, 'M_SP_TB_1': TypeID.M_SP_TB_1,
                'M_DP_TB_1': TypeID.M_DP_TB_1, 'M_ST_TB_1': TypeID.M_ST_TB_1,
                'M_BO_TB_1': TypeID.M_BO_TB_1, 'M_ME_TD_1': TypeID.M_ME_TD_1,
                'M_ME_TE_1': TypeID.M_ME_TE_1, 'M_ME_TF_1': TypeID.M_ME_TF_1,
                'M_IT_TB_1': TypeID.M_IT_TB_1, 'M_EP_TD_1': TypeID.M_EP_TD_1,
                'M_EP_TE_1': TypeID.M_EP_TE_1, 'M_EP_TF_1': TypeID.M_EP_TF_1,
                'C_SC_NA_1': TypeID.C_SC_NA_1, 'C_DC_NA_1': TypeID.C_DC_NA_1,
                'C_RC_NA_1': TypeID.C_RC_NA_1, 'C_SE_NA_1': TypeID.C_SE_NA_1,
                'C_SE_NB_1': TypeID.C_SE_NB_1, 'C_SE_NC_1': TypeID.C_SE_NC_1,
                'C_BO_NA_1': TypeID.C_BO_NA_1, 'C_SC_TA_1': TypeID.C_SC_TA_1,
                'C_DC_TA_1': TypeID.C_DC_TA_1, 'C_RC_TA_1': TypeID.C_RC_TA_1,
                'C_SE_TA_1': TypeID.C_SE_TA_1, 'C_SE_TB_1': TypeID.C_SE_TB_1,
                'C_SE_TC_1': TypeID.C_SE_TC_1, 'C_BO_TA_1': TypeID.C_BO_TA_1,
                'M_EI_NA_1': TypeID.M_EI_NA_1, 'C_IC_NA_1': TypeID.C_IC_NA_1,
                'C_CI_NA_1': TypeID.C_CI_NA_1, 'C_RD_NA_1': TypeID.C_RD_NA_1,
                'C_CS_NA_1': TypeID.C_CS_NA_1, 'C_TS_NA_1': TypeID.C_TS_NA_1,
                'C_RP_NA_1': TypeID.C_RP_NA_1, 'C_CD_NA_1': TypeID.C_CD_NA_1,
                'C_TS_TA_1': TypeID.C_TS_TA_1}

    @staticmethod
    def type_converter(iec_type) -> Type:
        """ {type_id -> (python) val_type: val_type expected for val for type_id set }"""
        converter: Dict[TypeID, type] = {
            TypeID.M_SP_NA_1: bool,
            TypeID.M_ST_NA_1: int,
            TypeID.M_BO_NA_1: int,
            TypeID.M_ME_NC_1: float,
            TypeID.C_SC_NA_1: bool,
            TypeID.C_RC_NA_1: int,
            TypeID.C_SE_NC_1: float,
            TypeID.P_AC_NA_1: bool,

            # new timestamp ones; for now only doing the process-value conversion
            TypeID.C_DC_NA_1: int,
            TypeID.M_SP_TB_1: bool,
            TypeID.M_ME_TD_1: float,
            TypeID.M_DP_TB_1: int,
            TypeID.M_ME_TE_1: int,
            TypeID.M_ME_TF_1: float,
        }
        return converter.get(iec_type, int)

    def convert_val_by_type(self, val) -> IEC_SINGLE_VALUE:
        """
        Converts val to its expected (python) datatype
        This _might_ not be its final value.
        (floats will be transformed into IEEE-RE32 floats which
            have a different accuracy than Python-floats)
        """
        return TypeID.type_converter(self)(val)

    @property
    def direction(self) -> MsgDirection:
        if '.' in str(self):
            _str = str(self).split('.')[1]
        else:
            _str = str(self)
        direction_char = _str.split('_')[0]
        return {
            'M': MsgDirection.MONITORING,
            'C': MsgDirection.CONTROL,
            'P': MsgDirection.UNKNOWN,
            'F': MsgDirection.UNKNOWN,
        }[direction_char]

    @property
    def global_coa_compatible(self) -> bool:
        """ If an APDU with this typeID is valid with the global COA 0xFFFF """
        return self in (TypeID.C_IC_NA_1, TypeID.C_CI_NA_1, TypeID.C_CS_NA_1, TypeID.C_RP_NA_1)

    @property
    def carries_normalised_value(self) -> bool:
        """
        Applies to ASDUs carrying IOs with an NVA (Normalised Value; Type 4.1)
        Other normalised values values of 60870-5-4 are not implemented by 60870-5-104
        """
        return self in (
                TypeID.M_ME_NA_1,
                TypeID.M_ME_TA_1,
                TypeID.M_ME_ND_1,
                TypeID.M_ME_TD_1,
                TypeID.C_SE_NA_1,
                TypeID.C_SE_TA_1,
                TypeID.P_ME_NA_1,
        )

    @property
    def carries_parameter_modification(self) -> bool:
        return 110 <= self.value <= 113

    @property
    def can_send_periodic_update(self) -> bool:
        """
        A Type can send periodic updates iff it defines a format of 'measured value' without timestamp
        (9, 11, 13, 21)
        """
        return self in (
            TypeID.M_ME_NA_1,
            TypeID.M_ME_NB_1,
            TypeID.M_ME_NC_1,
            TypeID.M_ME_ND_1,
        )

    def __eq__(self, other):
        if isinstance(other, int):
            return self.value == other
        if isinstance(other, TypeID):
            return self.value == other.value
        raise TypeError(f"'==' not supported between instances of '{type(other)}' and 'TypeID'")

    def __lt__(self, other):
        if isinstance(other, int):
            return self.value < other
        if isinstance(other, TypeID):
            return self.value < other.value
        raise TypeError(f"'<' not supported between instances of '{type(other)}' and 'TypeID'")

    def __gt__(self, other):
        if isinstance(other, int):
            return self.value > other
        if isinstance(other, TypeID):
            return self.value > other.value
        raise TypeError(f"'>' not supported between instances of '{type(other)}' and 'TypeId'")

    def __hash__(self):
        return hash(self.value)


