from enum import unique, IntEnum

from wattson.iec104.interface.types.apdu_direction import APDUDirection


@unique
class COT(IntEnum):
    # UNDEFINED = 0     TODO: Should make this none where it is necessary
    PERIODIC = 1
    BACKGROUND_INTERROGATION = 2
    SPONTANEOUS = 3
    INITIALIZED = 4
    INTERROGATION = 5
    ACTIVATION = 6
    ACTIVATION_CONFIRMATION = 7
    DEACTIVATION = 8
    DEACTIVATION_CONFIRMATION = 9
    ACTIVATION_TERMINATION = 10

    RESPONSE_TO_REMOTE_COMMAND = 11
    RESPONSE_TO_LOCAL_COMMAND = 12

    DATA_TRANSMISSION = 13

    INTERROGATED_BY_STATION = 20    # AKA General Interrogation reply
    INTERROGATED_BY_GROUP_1 = 21
    INTERROGATED_BY_GROUP_2 = 22
    INTERROGATED_BY_GROUP_3 = 23
    INTERROGATED_BY_GROUP_4 = 24
    INTERROGATED_BY_GROUP_5 = 25
    INTERROGATED_BY_GROUP_6 = 26
    INTERROGATED_BY_GROUP_7 = 27
    INTERROGATED_BY_GROUP_8 = 28
    INTERROGATED_BY_GROUP_9 = 29
    INTERROGATED_BY_GROUP_10 = 30
    INTERROGATED_BY_GROUP_11 = 31
    INTERROGATED_BY_GROUP_12 = 32
    INTERROGATED_BY_GROUP_13 = 33
    INTERROGATED_BY_GROUP_14 = 34
    INTERROGATED_BY_GROUP_15 = 35
    INTERROGATED_BY_GROUP_16 = 36

    COUNTER_INTERROGATION_GENERAL = 37
    COUNTER_INTERROGATION_GROUP_1 = 38
    COUNTER_INTERROGATION_GROUP_2 = 39
    COUNTER_INTERROGATION_GROUP_3 = 40
    COUNTER_INTERROGATION_GROUP_4 = 41

    UNKNOWN_TYPE = 44
    UNKNOWN_CAUSE = 45
    UNKNOWN_COA = 46
    UNKNOWN_IOA = 47

    @property
    def confirm_cot(self) -> 'COT':
        if self == COT.ACTIVATION:
            return COT.ACTIVATION_CONFIRMATION
        elif self == COT.DEACTIVATION:
            return COT.DEACTIVATION_CONFIRMATION
        else:
            raise ValueError(f"There is no confirmation for the cot {self}.")

    @property
    def is_unknown_COT(self) -> bool:
        return self in range(44, 48)

    @property
    def is_known_COT(self) -> bool:
        return not self.is_unknown_COT

    @property
    def default_direction(self) -> APDUDirection:
        if (self.confirm_cot
            or self in (COT.ACTIVATION_TERMINATION, COT.SPONTANEOUS, COT.PERIODIC)
            or self in range(COT.INTERROGATED_BY_STATION, COT.INTERROGATED_BY_GROUP_16 + 1)
            or self.is_unknown_COT
        ):
            return APDUDirection.MONITORING
        elif self in (COT.ACTIVATION, COT.DEACTIVATION):
            return APDUDirection.CONTROL
        else:
            # COT 5 = Interrogation for both used
            return APDUDirection.BOTH_POSSIBLE

    def __eq__(self, other):
        if isinstance(other, int):
            return self.value == other
        if isinstance(other, COT):
            return self.value == other.value
        raise TypeError(f"'==' not supported between instances of '{type(other)}' and 'COT'")

    def __lt__(self, other):
        if isinstance(other, int):
            return self.value < other
        if isinstance(other, COT):
            return self.value < other.value
        raise TypeError(f"'<' not supported between instances of '{type(other)}' and 'COT'")

    def __gt__(self, other):
        if isinstance(other, int):
            return self.value > other
        if isinstance(other, COT):
            return self.value > other.value
        raise TypeError(f"'>' not supported between instances of '{type(other)}' and 'COT'")

    def __hash__(self):
        return hash(self.value)
