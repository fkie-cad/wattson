import logging
from typing import List, TYPE_CHECKING, Union, Optional

import iec61850_python
from wattson.iec61850.common.iec61850_helpers import is_error
from wattson.util import get_logger

if TYPE_CHECKING:
    from wattson.iec61850.iec61850_model import IEC61850Model
    from wattson.iec61850.iec61850_logical_node import IEC61850LogicalNode


class IEC61850LogicalDevice:
    def __init__(self,
                 lib_object: iec61850_python.LogicalDevice,
                 model: Optional['IEC61850Model'] = None,
                 logical_nodes: Optional[List['IEC61850LogicalNode']] = None) -> None:

        if logical_nodes is None:
            logical_nodes = []
        self.model = model
        self.lib_object = lib_object
        self.logical_nodes = logical_nodes

    def get_model(self) -> 'IEC61850Model':
        return self.model

    def add_logical_node(self, logical_node: 'IEC61850LogicalNode') -> bool:
        if self.has_logical_node(logical_node):
            return False
        self.logical_nodes.append(logical_node)
        logical_node.logical_device = self
        return True

    def get_logical_nodes(self) -> List['IEC61850LogicalNode']:
        return self.logical_nodes

    def get_logical_node(self, logical_node: Union[str, 'IEC61850LogicalNode']) -> 'IEC61850LogicalNode':
        if isinstance(logical_node, str):
            for logical_node_object in self.logical_nodes:
                if logical_node_object.name == logical_node:
                    return logical_node_object
            raise KeyError(f"Logical node {logical_node} does not exist")
        if self.has_logical_node(logical_node):
            return logical_node

    def has_logical_node(self, logical_node: Union[str, 'IEC61850LogicalNode']) -> bool:
        if isinstance(logical_node, str):
            for logical_node_object in self.logical_nodes:
                if logical_node_object.name == logical_node:
                    return True
            return False
        return logical_node in self.logical_nodes

    def ensure_logical_node(self, logical_node_name: str) -> 'IEC61850LogicalNode':
        """
        Searches for the given logical node name and creates the respective node if it does not exist.

        Args:
            logical_node_name (str):
                The logical node name

        Returns:
            'IEC61850LogicalNode': The IEC61850LogicalNode
        """
        from wattson.iec61850.iec61850_logical_node import IEC61850LogicalNode
        if self.has_logical_node(logical_node_name):
            return self.get_logical_node(logical_node_name)
        lib_logical_node = self.lib_object.add_logical_node(logical_node_name)
        logical_node = IEC61850LogicalNode(lib_logical_node)
        self.add_logical_node(logical_node)
        return logical_node

    @property
    def name(self) -> str:
        return self.lib_object.get_name()

    @property
    def reference(self) -> str:
        return self.name

    def build_from_connection(self, connection: iec61850_python.Connection, logger: logging.Logger):
        nodes, error = connection.get_logical_device_directory(self.name)
        if is_error(error):
            logger.error(f"Failed while retrieving device directory for {self.name}: {error=}")
            return False
        logger.debug(f"Device {self.name} has {len(nodes)} nodes")

        for node_name in nodes:
            logger.debug(f"Creating node {node_name}")
            logical_node = self.ensure_logical_node(node_name)
            logger.debug(f"Created node {node_name}")
            if not logical_node.build_from_connection(connection, logger):
                return False
        return True
