from wattson.powergrid.simulator.messages.power_grid_query import PowerGridQuery
from wattson.powergrid.simulator.messages.power_grid_query_type import PowerGridQueryType


class PowerGridMeasurementQuery(PowerGridQuery):
    def __init__(self, element_identifier: str, attribute_context: str, attribute_name: str):
        super().__init__(query_type=PowerGridQueryType.MEASUREMENT,
                         query_data={
                             "element_identifier": element_identifier,
                             "attribute_context": attribute_context,
                             "attribute_name": attribute_name
                         })

    @property
    def element_identifier(self) -> str:
        return self.query_data["element_identifier"]

    @property
    def attribute_context(self) -> str:
        return self.query_data["attribute_context"]

    @property
    def attribute_name(self) -> str:
        return self.query_data["attribute_name"]
