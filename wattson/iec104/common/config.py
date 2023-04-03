COA_LENGTH = 2
IOA_LENGTH = 3

SERVER_DEFAULT_PORT = 2404
SERVER_TICK_RATE_MS = 2000
# interval in seconds used to send periodic updates (COT=1) from server to client
SERVER_UPDATE_PERIOD_S = 10
SERVER_UPDATE_PERIOD_MS = SERVER_UPDATE_PERIOD_S * 1000

CLIENT_COMMAND_TIMEOUT_MS = 2000
CLIENT_TICKRATE_MS = 1000
CLIENT_UPDATE_INTERVAL_S = 1
MTU_UPDATE_INTERVAL_S = 5
MTU_DEFAULT_CONNECTION_WAIT_S = 10
MTU_READY_EVENT = "MTU_READY"

# TODO: haven't found the right number in the norm yet
CLIENT_CLOCK_SYNCH_TICKRATE_S = 10

# supported ASDU types: single command, step position and IEEE float for MD and
# CD plus some control types as 100 (interrogation), clock-synch
# TODO: for type 3 and 31 waiting for c104 update, because in monitoring direction we
#      the client only forwards if the new status != 3, instead of the resp. value in {0,1,2,3}
SUPPORTED_ASDU_TYPES = {1, 3, 5, 7, 13} \
                       | {30, 31, 35, 36}\
                       | {45, 46, 47, 50}\
                       | {100, 102, 103, 113}
# short floating point = IEE float

APCI_PARAMETERS = {
    'k': 12,
    'w': 8,
    't0': 10,
    't1': 15,
    't2': 10,
    't3': 20,
}

"""
Equal to the following: (not used due to circular imports)
SUPPORTED_ASDU_TYPES = {TypeID.M_SP_NA_1, TypeID.M_ST_NA_1, TypeID.M_ME_NC_1,
                        TypeID.C_SC_NA_1, TypeID.C_RC_NA_1, TypeID.C_RC_NA_1,
                        TypeID.C_IC_NA_1, TypeID.C_RD_NA_1, TypeID.C_CS_NA_1,
                        TypeID.P_AC_NA_1,
                        }
"""

SUPPORTED_COTS = {1, 3, 5, 20} | {5, 6, 7, 8, 9, 10} | {44, 45, 46, 47}

"""
Equal to: 
SUPPORTED_COTS = {
    COT.PERIODIC, COT.SPONTANEOUS, COT.INTERROGATION, COT.INTERROGATED_BY_STATION,
    COT.ACTIVATION, COT.ACTIVATION_CONFIRMATION, COT.ACTIVATION_TERMINATION,
    # DEACT only for P_AC
    COT.DEACTIVATION, COT.DEACTIVATION_CONFIRMATION,
    # only for reply on a bad recvd ASDU
    COT.UNKNOWN_IOA, COT.UNKNOWN_COA, COT.UNKNOWN_CAUSE,
}
"""