from typing import Any

from wattson.cosimulation.control.messages.wattson_response import WattsonResponse


class PowerGridMeasurementResponse(WattsonResponse):
    def __init__(self, element_identifier: str, attribute_name: str, value: Any, successful: bool = True):
        super().__init__(successful=successful,
                         data={
                            "element_identifier": element_identifier,
                            "attribute_name": attribute_name,
                            "attribute_context": "",
                            "value": value
                         })

    @property
    def element_identifier(self) -> str:
        return self.data["element_identifier"]

    @property
    def attribute_context(self) -> str:
        return self.data["attribute_context"]

    @property
    def attribute_name(self) -> str:
        return self.data["attribute_name"]

    @property
    def value(self) -> Any:
        return self.data["value"]
