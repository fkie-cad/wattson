from typing import List

import c104

from wattson.iec104.interface.types import TypeID, COT
from wattson.iec104.interface.apdus import (
    APDU, IEC60870_TYPEID_OFFSET, IEC60870_MSGINFO_OFFSET,
    I_FORMAT, U_FORMAT, S_FORMAT
)
from wattson.iec104.common import GLOBAL_COA


def build_apdu_from_c104_bytes(data: bytes) -> APDU:
    """
    Builds basic APDU from a given raw message

    Args:
        data (bytes):
            
    """

    msg = explain_bytes(data)
    expl = [s.strip() for s in msg.split("|")]
    if expl[0] == "I-Format":
        type_ID = TypeID(data[IEC60870_TYPEID_OFFSET])
        cot = COT(int(data[IEC60870_MSGINFO_OFFSET] & 0b00111111))
        pos = expl[3] == "POSITIVE"
        coa = GLOBAL_COA if expl[4] == "GLOBAL" else int(expl[4].split()[1])
        ioas = extract_ioas(data)
        return I_FORMAT(type_ID, cot, coa, pos, ioas)
    if expl[0] == "U-Format":
        return U_FORMAT(expl[1])
    return S_FORMAT(data)


def extract_ioa(data: bytes) -> int:
    """
    Currently assumes just <= 1 IO

    Args:
        data (bytes):
            
    """
    expl = [s.strip() for s in explain_bytes(data).split("|")]
    if "OBJECT" not in expl[-1]:
        return 0
    ioa_hex = "".join(expl[-1][8:-1].split(" ")[2::-1])
    ioa = int(ioa_hex, 16)
    return ioa


def extract_ioas(data: bytes) -> List[int]:
    #expl = [s.strip() for s in explain_bytes(data).split("|")]
    expl_dict = c104.explain_bytes_dict(data)
    if expl_dict["numberOfObjects"] <= 0:
        raise ValueError(f"APDU contains no object.")

    io_hex = bytes.fromhex(expl_dict["elements"])

    #io_hex = bytes.fromhex(explain_bytes(data).split("|")[-1][9:-2])

    apdu_len = data[1]
    sequence = data[7] & 0x80
    num_ios = data[7] & 0x7F
    IOA_len = 3
    header_len = 10
    ie_len = int((apdu_len - header_len) / num_ios)
    if sequence:
        first_ioa = int.from_bytes(io_hex[0:3], byteorder="little")
        return [first_ioa + i for i in range(num_ios)]
    else:
        ioas = [
            int.from_bytes(io_hex[i * ie_len: i * ie_len + IOA_len], byteorder="little")
            for i in range(num_ios)
        ]
        return ioas


def explain_bytes(data: bytes):
    return c104.explain_bytes(apdu=data)
