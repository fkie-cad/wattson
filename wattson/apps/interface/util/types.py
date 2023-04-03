from typing import Union

from wattson.apps.interface.util.messages import (
    SubscriptionInitReply,
    SubscriptionInitMsg,
    IECMsg,
    ConnectionStatusChange
)

SUBSCRIPTION_MSG = Union[SubscriptionInitReply, SubscriptionInitMsg]
ALL_MSGS = Union[IECMsg, SUBSCRIPTION_MSG, ConnectionStatusChange]

RECV_SEND_TYPES = Union[ALL_MSGS, str]  # STR_NO_RESPONSE
