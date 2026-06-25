import logging
from typing import List, TYPE_CHECKING, Optional, Union, Dict, Callable, Any, Tuple

import iec61850_python
from networkx.algorithms.isomorphism.tree_isomorphism import root_trees

from powerowl.layers.network.configuration.protocols.iec61850.mms_functional_constraints import MMSFunctionalConstraints
from wattson.iec61850.common.iec61850_helpers import is_error
from wattson.iec61850.common.model_lock import ModelLock
from wattson.iec61850.iec61850_data_object import IEC61850DataObject
from wattson.util import get_logger

if TYPE_CHECKING:
    from wattson.iec61850.iec61850_logical_device import IEC61850LogicalDevice
    from wattson.iec61850.iec61850_report_control_block import IEC61850ReportControlBlock
    from wattson.iec61850.iec61850_data_attribute import IEC61850DataAttribute
    from wattson.iec61850.iec61850_data_set import IEC61850DataSet


class IEC61850Model:
    def __init__(self,
                 lib_object: Union[str, iec61850_python.Model],
                 server_id: int,
                 logical_devices: Optional[List['IEC61850LogicalDevice']] = None):
        if logical_devices is None:
            logical_devices = []

        if isinstance(lib_object, str):
            lib_object = iec61850_python.Model(lib_object)
        self.server_id = server_id
        self.lib_object = lib_object
        self.logical_devices = logical_devices
        self._connection: Optional[iec61850_python.Connection] = None
        self._data_points: Dict[str, 'IEC61850DataAttribute'] = {}
        self._lib_server = None
        self.model_lock = ModelLock(self)
        self._control_objects: Dict[str, iec61850_python.ControlObject] = {}

    def is_remote(self) -> bool:
        return self._connection is not None

    def link_server(self, server: iec61850_python.Server):
        self._lib_server = server

    def lock_model(self):
        if self._lib_server is None:
            return
        self._lib_server.lock_data_model()

    def unlock_model(self):
        if self._lib_server is None:
            return
        self._lib_server.unlock_data_model()

    def update_data_point_values(self, get_value_callback: Callable[[str], Any], initial_values: List[Tuple['IEC61850DataAttribute', Any]]):
        with self.model_lock:
            for data_attribute, initial_value in initial_values:
                # print(f"{data_attribute.get_mms_path()} -> {initial_value}", flush=True)
                if not data_attribute.update_model_value(initial_value):
                    print(f" Failed to set initial value!")
            for data_point_identifier, data_attribute in self._data_points.items():
                if data_attribute.is_measurement():
                    try:
                        initial_value = get_value_callback(data_point_identifier)
                        # print(f"{data_attribute.get_mms_path()} -> {initial_value}")
                        data_attribute.update_model_value(initial_value)
                    except Exception as e:
                        continue

    @property
    def connection(self) -> Optional[iec61850_python.Connection]:
        return self._connection

    def get_logical_devices(self):
        return self.logical_devices

    def add_logical_device(self, logical_device: 'IEC61850LogicalDevice') -> bool:
        if self.has_logical_device(logical_device):
            return False
        logical_device.model = self
        self.logical_devices.append(logical_device)
        return True

    def get_logical_device(self, device_name: str) -> 'IEC61850LogicalDevice':
        for logical_device in self.logical_devices:
            if logical_device.name == device_name:
                return logical_device
        raise KeyError(f'Logical device {device_name} not found')

    def has_logical_device(self, device: Union[str, 'IEC61850LogicalDevice']) -> bool:
        if isinstance(device, str):
            for logical_device in self.logical_devices:
                if logical_device.name == device:
                    return True
            return False
        return device in self.logical_devices

    def ensure_logical_device(self, logical_device_name: str) -> 'IEC61850LogicalDevice':
        """
        Searches for the given logical device name and creates the respective device if it does not exist.

        Args:
            logical_device_name (str):
                The logical device name

        Returns:
            'IEC61850LogicalDevice': The IEC61850LogicalDevice
        """
        from wattson.iec61850.iec61850_logical_device import IEC61850LogicalDevice
        if self.has_logical_device(logical_device_name):
            return self.get_logical_device(logical_device_name)
        lib_logical_device = self.lib_object.add_logical_device(logical_device_name)
        logical_device = IEC61850LogicalDevice(lib_logical_device)
        self.add_logical_device(logical_device)
        return logical_device

    @property
    def name(self) -> str:
        return self.lib_object.get_ied_name()

    def get_server_id(self) -> int:
        return self.server_id

    def find_report_control_block(self, report_control_block_reference: str) -> 'IEC61850ReportControlBlock':
        path = report_control_block_reference
        parts = path.split("/")
        device = parts[0]
        end_parts = parts[1].split(".")
        node = end_parts[0]
        rpc_id = end_parts[2]
        if not self.has_logical_device(device):
            raise KeyError(f'Logical device {device} not found - got {[ld.name for ld in self.get_logical_devices()]}')
        if not self.get_logical_device(device).has_logical_node(node):
            raise KeyError(f'Logical node {node} not found')
        node = self.get_logical_device(device).get_logical_node(node)
        if not node.has_report_control_block(rpc_id):
            raise KeyError(f'Report control block {rpc_id} not found')
        return node.get_report_control_block(rpc_id)

    def get_data_attributes(self) -> List['IEC61850DataAttribute']:
        attributes = []
        for logical_device in self.get_logical_devices():
            for logical_node in logical_device.get_logical_nodes():
                attributes.extend(logical_node.get_data_attributes())
        return attributes

    def get_data_sets(self) -> List['IEC61850DataSet']:
        data_sets = []
        for logical_device in self.get_logical_devices():
            for logical_node in logical_device.get_logical_nodes():
                data_sets.extend(logical_node.get_data_sets())
        return data_sets

    def get_child_by_path(self, path: str) -> Optional[Union['IEC61850DataAttribute', 'IEC61850DataObject']]:
        path = path.removeprefix(self.name)
        parts = path.split("/")
        if len(parts) != 2:
            return None
        ld_name = parts[0]
        path = parts[1].split(".")
        ln_name = path.pop(0)
        if not self.has_logical_device(ld_name):
            return None
        logical_device = self.get_logical_device(ld_name)
        if not logical_device.has_logical_node(ln_name):
            return None
        logical_node = logical_device.get_logical_node(ln_name)
        return logical_node.get_child_by_path(path)

    def register_data_point(self, data_point_identifier: str, data_attribute: 'IEC61850DataAttribute'):
        self._data_points[data_point_identifier] = data_attribute

    def get_data_attribute_by_data_point_identifier(self, data_point_identifier: str) -> Optional['IEC61850DataAttribute']:
        return self._data_points.get(data_point_identifier)

    def clear(self):
        self.logical_devices = []
        self._data_points = {}
        self._connection = None

    def build_from_connection(self, connection: iec61850_python.Connection, logger: Optional[logging.Logger] = None) -> bool:
        self.clear()
        if logger is None:
            logger = get_logger(f"MMS-Model-{self.name}")
            logger.setLevel(logging.CRITICAL)
        logger.setLevel(logging.DEBUG)

        self._connection = connection

        connection.get_device_model_from_server()

        devices, error = connection.get_server_directory()
        if is_error(error):
            logger.error(f"Failed while retrieving device list: {error=}")
            return False
        logger.debug(f"Found {len(devices)} devices")
        for device_name in devices:
            logical_device = self.ensure_logical_device(device_name)
            if not logical_device.build_from_connection(connection, logger):
                return False
        return True
