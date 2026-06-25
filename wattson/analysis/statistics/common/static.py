from typing import Optional

from wattson.analysis.statistics.common.statistic_message import StatisticMessage
from wattson.cosimulation.control.messages.wattson_query import WattsonQuery
from wattson.cosimulation.control.messages.wattson_query_type import WattsonQueryType


class StaticStatisticClient:
    enabled: bool = False
    server_ip: Optional[str] = None
    instance: Optional['StaticStatisticClient'] = None

    def __init__(self):
        self._wattson_client = None
        if StaticStatisticClient.server_ip is None:
            StaticStatisticClient.enabled = False
        if StaticStatisticClient.enabled:
            from wattson.cosimulation.control.interface.wattson_client import WattsonClient
            self._wattson_client = WattsonClient(namespace="auto", wattson_socket_ip=StaticStatisticClient.server_ip)
            self._wattson_client.start()

    @staticmethod
    def stop():
        if StaticStatisticClient.instance is not None:
            if StaticStatisticClient.instance._wattson_client is not None:
                StaticStatisticClient.instance._wattson_client.stop()

    @staticmethod
    def get_instance() -> 'StaticStatisticClient':
        if StaticStatisticClient.instance is None:
            StaticStatisticClient.instance = StaticStatisticClient()
        return StaticStatisticClient.instance

    @staticmethod
    def emit(statistic_message: StatisticMessage):
        if StaticStatisticClient.enabled:
            instance = StaticStatisticClient.get_instance()
            instance._wattson_client.async_query(WattsonQuery(
                query_type=WattsonQueryType.SUBMIT_STATISTIC,
                query_data=statistic_message.to_dict()
            ))
