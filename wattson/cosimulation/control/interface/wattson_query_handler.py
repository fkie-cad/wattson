import abc
from typing import Type, Union, Optional

from wattson.cosimulation.control.messages.wattson_query import WattsonQuery
from wattson.cosimulation.control.messages.wattson_response import WattsonResponse


class WattsonQueryHandler(abc.ABC):
    @staticmethod
    def get_simulation_query_type(
            query: Union[WattsonQuery, Type[WattsonQuery]]
    ) -> Type[WattsonQuery]:

        if isinstance(query, WattsonQuery):
            return query.__class__
        elif issubclass(query, WattsonQuery):
            return query
        raise ValueError("Invalid query type")

    @abc.abstractmethod
    def handles_simulation_query_type(self, query: Union[WattsonQuery, Type[WattsonQuery]]) -> bool:
        """
        Checks whether the physical simulator handles specific SimulationControlQueries.

        Args:
            query (Union[WattsonQuery, Type[WattsonQuery]]):
                The query instance or class to be checked.

        Returns:
            bool: Whether this simulator can handle this query type
        """
        ...

    @abc.abstractmethod
    def handle_simulation_control_query(self, query: WattsonQuery) -> Optional[WattsonResponse]:
        """
        Handles the given WattsonQuery and provides an optional response.
        If the query type is not supported, this should raise an InvalidSimulationControlQueryException.

        Args:
            query (WattsonQuery):
                
        """
        ...
