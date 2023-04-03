from abc import ABC

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from wattson.powergrid.server.coord_server import CoordinationServer


class CoordinatorLogicInterface(ABC):
    def __init__(self, coordinator: 'CoordinationServer', args: dict = None):
        if not args:
            args = {}
        self.args = args
        self.coordinator = coordinator

    def get_priority(self):
        """
        Returns a priority index - the higher the index, the later the transform functions are called.

        :return: The priority index of the transform script.
        """
        return 0

    def setup(self, net):
        """
        Called before the simulation starts to apply any changes to the grid for instantiation.

        :param net: The power net with write permissions
        :return: None
        """
        pass

    def pre_sim_transform(self, net):
        """
        Called before every simulation step.

        :param net: The PandaPower Network with exclusive write permissions
        :return: None
        """
        pass

    def post_sim_transform(self, net):
        """
        Called after every simulation step.

        :param net: The PandaPower Network with exclusive write permissions
        :return: None
        """
        pass

    def write_transform(self, net, table, index, column, value) -> bool:
        """
        Allows to change or expand write queries on the grid by the coordinator to realize static logic.

        The net can be written arbitrarily, while the return value indicates if the default action should be prevented.

        :param net: The PandaPower Network with exclusive write permissions.
        :param table: The table that the original write request targets.
        :param index: The index that the original write request targets.
        :param column: The column that the original write request targets.
        :param value: The value that should be written to the net
        :return: Whether to prevent the default write action from the Coordinator.
        """
        return False

    def read_transform(self, net, table, index, column, default_value):
        """
        Allows to change the result of a read request issued to the coordinator.

        Further, the net can be altered arbitrarily.

        :param net: The PandaPower Network with exclusive read permissions.
        :param table: The table that the original read request targets.
        :param index: The index that the original read request targets.
        :param column: The column that the original read request targets.
        :param default_value: The value that would be returned by default.
        :return: The value to be emitted for the read request by the coordinator.
        """
        return default_value
