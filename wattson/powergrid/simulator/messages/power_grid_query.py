from wattson.cosimulation.control.messages.wattson_query import WattsonQuery


class PowerGridQuery(WattsonQuery):
    def requires_native_namespace(self) -> bool:
        return False
