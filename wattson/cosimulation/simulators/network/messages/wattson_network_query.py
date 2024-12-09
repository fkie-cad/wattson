from wattson.cosimulation.control.messages.wattson_query import WattsonQuery


class WattsonNetworkQuery(WattsonQuery):
    def requires_native_namespace(self) -> bool:
        return True
