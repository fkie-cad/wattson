import abc
from typing import TYPE_CHECKING

from wattson.hosts.ccx.app_gateway.messages.app_gateway_notification import AppGatewayNotification

if TYPE_CHECKING:
    from wattson.hosts.ccx.app_gateway import AppGatewayClient


class NotificationHandler(abc.ABC):
    def __init__(self, app_gateway: 'AppGatewayClient'):
        self.app_gateway = app_gateway
        self._priority = 0

    def set_priority(self, priority: int):
        self._priority = priority

    @property
    def priority(self):
        return self._priority

    def handle(self, notification: AppGatewayNotification) -> bool:
        """
        Attempts to handle the given notification.
        If it is handled, True should be returned.
        If it is not handled, False should be returned.

        Args:
            notification (AppGatewayNotification):
                The received AppGatewayNotification

        Returns:
            bool: Whether the notification has been handled
        """
        return False
