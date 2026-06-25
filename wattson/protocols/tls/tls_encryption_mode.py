import enum


class TlsEncryptionMode(str, enum.Enum):
    NONE = "NONE"
    ENABLED = "ENABLED"
