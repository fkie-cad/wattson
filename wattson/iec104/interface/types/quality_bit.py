from enum import unique, IntEnum


@unique
class QualityBit(IntEnum):
    # object + quality descriptors are described in IEC-101; part 7.2.6
    # SIQ: see 7.2.6.1
    # DIQ: see 7.2.6.3
    # QDP: see 7.2.6.4
    # or the other way around? (INVALID = 64, ...)
    INVALID = 0x80
    NON_TOPICAL = 0x40
    SUBSTITUTED = 0x20
    BLOCKED = 0x10
    ELAPSED_TIME_INVALID = 0x08
    RESERVED = 0x04
    OVERFLOW = 0x01

    # only indirectly used, not part of iec101
    GOOD = 0x00
