from enum import IntEnum


class ReferenceType(IntEnum):
    CmdReply = 1
    CmdUpdate = 2
    MTUUpdate = 3
    ExternalUpdate = 4
    DPUpdate = 5

    def to_queue_name(self):
        conversion = {
            ReferenceType.CmdReply: "cmd_reply",
            ReferenceType.CmdUpdate: "cmd_update",
            ReferenceType.MTUUpdate: "mtu_update",
            ReferenceType.ExternalUpdate: "external_update",
            ReferenceType.DPUpdate: "cmd_update"
        }
        return conversion[self]