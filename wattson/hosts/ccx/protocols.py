import enum


class CCXProtocol(str, enum.Enum):
    UNKNOWN = "UNKNOWN"

    IEC104 = "60870-5-104"
    IEC61850_MMS = "61850-MMS"
    #IEC61850_RCB = "61850-rcb"
