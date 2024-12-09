from wattson.hosts.ccx.app_gateway.handlers.notification_handler import NotificationHandler
from wattson.hosts.ccx.app_gateway.messages.app_gateway_message_type import AppGatewayMessageType
from wattson.hosts.ccx.app_gateway.messages.app_gateway_notification import AppGatewayNotification


class DefaultNotificationHandler(NotificationHandler):
    def handle(self, notification: AppGatewayNotification) -> bool:
        n_type = notification.notification_type
        n_data = notification.notification_data

        if n_type == AppGatewayMessageType.DATA_POINT_RECEIVED:
            self.app_gateway.trigger(
                "data_point_received",
                n_data.get("protocol"),
                n_data.get("data_point_identifier"),
                n_data.get("value"),
                n_data.get("protocol_data")
            )
            return True

        if n_type == AppGatewayMessageType.CONNECTION_CHANGE:
            self.app_gateway.trigger(
                "connection_change",
                n_data.get("protocol"),
                n_data.get("server_key"),
                n_data.get("server_ip"),
                n_data.get("server_port"),
                n_data.get("connection_status")
            )
            return True

        return False
