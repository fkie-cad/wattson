import threading
import time
from typing import List, TYPE_CHECKING, Optional, Union, Any, Callable

import iec61850_python

from powerowl.layers.network.configuration.protocols.iec61850.mms_functional_constraints import MMSFunctionalConstraints
from powerowl.layers.network.configuration.protocols.iec61850.mms_trigger_options import MMSTriggerOptions
from wattson.analysis.statistics.common.static import StaticStatisticClient
from wattson.analysis.statistics.common.statistic_message import StatisticMessage
from wattson.iec61850.common.iec61850_helpers import is_error
from wattson.iec61850.common.mms_error import MmsError
from wattson.iec61850.iec61850_data_attribute import IEC61850DataAttribute
from wattson.iec61850.iec61850_mms_value import IEC61850MMSValue

if TYPE_CHECKING:
    from wattson.iec61850.iec61850_data_object import IEC61850DataObject


class IEC61850RemoteDataAttribute(IEC61850DataAttribute):
    def __init__(self,
                 name: str,
                 mms_type: iec61850_python.MmsType,
                 functional_constraint: MMSFunctionalConstraints,
                 parent: Optional[Union['IEC61850DataObject', 'IEC61850RemoteDataAttribute']] = None,
                 children: Optional[List['IEC61850RemoteDataAttribute']] = None) -> None:

        super().__init__(None, functional_constraint)
        if children is None:
            children = []
        self._name = name
        self.parent = parent
        self.children = children
        self.functional_constraint = functional_constraint
        self.mms_type = mms_type
        self._last_value: Any = None
        self._on_change_callbacks: List[Callable[['IEC61850RemoteDataAttribute', Any, Any], None]] = []
        self._on_update_callbacks: List[Callable[['IEC61850RemoteDataAttribute', Any, Any], None]] = []

    @property
    def name(self) -> str:
        return self._name

    def get_mms_value_type(self) -> iec61850_python.MmsType:
        return self.mms_type

    def get_mms_integer_size(self) -> int:
        return 64

    def read_value_from_server(self) -> Any:
        if not self.get_model().is_remote():
            raise RuntimeError("Cannot read value from server without connection")
        connection = self.get_model().connection
        _start_time = time.perf_counter()
        mms_value, error = connection.read_object(self.get_attribute_reference(), self.get_library_functional_constraint())
        if is_error(error):
            raise MmsError("Could not read value from server")
        _end_time = time.perf_counter()
        StaticStatisticClient.emit(StatisticMessage(event_class="61850-mms-read", event_name=f"read-{self.get_mms_path()}", value=_end_time - _start_time))
        self.set_value(mms_value.get())
        return mms_value.get()

    def async_read_value_from_server(self,
                                     callback: Callable[[bool, 'IEC61850RemoteDataAttribute', Any, Optional[str]], None],
                                     custom_id: Optional[str] = None) -> bool:
        """
        Attempts to asynchronously read the object from the server and update its value.
        This method returns before the request finishes. When the request cannot be created, the callback is not called.

        Args:
            callback (Callable[[bool, 'IEC61850RemoteDataAttribute', Any, Optional[str]], None]):
                The callback to call when the response is ready. Parameters: success, data_attribute, new_value, custom_id
            custom_id (Optional[str], optional):
                An optional custom ID to be passed to the callback to identify different calls.
                (Default value = None)

        Returns:
            bool: True iff the request has been successfully created
        """

        if not self.get_model().is_remote():
            raise RuntimeError("Cannot read value from server without connection")
        model = self.get_model()
        _start_timestamp = time.time()
        _start_time = time.perf_counter()

        def _callback(connection: iec61850_python.Connection, error: iec61850_python.IedClientError, value: iec61850_python.MmsValue) -> None:
            if is_error(error):
                callback(False, self, None, custom_id)
                return
            _end_time = time.perf_counter()
            StaticStatisticClient.emit(
                StatisticMessage(event_class="61850-mms-read", event_name=f"read-{self.get_attribute_reference()}", value=_end_time - _start_time)
            )
            self.set_value(value.get())
            callback(True, self, self.get_last_value(), custom_id)
            return

        invoke_id, invoke_error = model.connection.read_object_async(self.get_attribute_reference(), self.get_library_functional_constraint(), _callback)
        if is_error(invoke_error):
            print(f"Invoke Error: {invoke_error=}", flush=True)
            # callback(False, self, None, custom_id)
        StaticStatisticClient.emit(StatisticMessage(event_class="61850-mms-read", event_name=f"read-send-{self.get_attribute_reference()}", value=_start_timestamp))
        return not is_error(invoke_error)

    def write_value_to_server(self, value: Any) -> bool:
        """
        Sends a command to the server to update the attribute's value.

        Args:
            value (Any):
                The value to write

        Returns:
            bool: Whether the writing was successful
        """
        if not self.get_model().is_remote():
            raise RuntimeError("Cannot write value to server without connection")
        connection = self.get_model().connection
        try:
            mms_value = IEC61850MMSValue.from_mms_value_type(value, self.mms_type)
        except ValueError as e:
            # TODO: Raise again?
            return False

        error = connection.write_object(self.get_attribute_reference(), self.get_library_functional_constraint(), mms_value.lib_object)
        if is_error(error):
            return False
        # Update value
        # TODO: Use setter or private attribute?
        # self._last_value = value
        self.set_value(value)
        return True

    def async_write_value_to_server(self,
                                    value: Any,
                                    callback: Callable[[bool, 'IEC61850RemoteDataAttribute', Optional[str]], None],
                                    custom_id: Optional[str] = None) -> bool:
        """
        Sends a command to the server to update the attribute's value.

        Args:
            value (Any):
                The value to write
            callback (Callable[[bool, 'IEC61850RemoteDataAttribute', Optional[str]], None]):
                The callback to call when the response is ready. Parameters: success, data_attribute, custom_id
            custom_id (Optional[str], optional):
                An optional custom ID to be passed to the callback to identify different calls.
                (Default value = None)

        Returns:
            bool: Whether the command could be sent - this is not an acknowledgement!
        """
        if not self.get_model().is_remote():
            raise RuntimeError("Cannot write value to server without connection")
        model = self.get_model()
        try:
            mms_value = IEC61850MMSValue.from_mms_value_type(value, self.mms_type)
        except ValueError as e:
            callback(False, self, custom_id)
            return False

        def _callback(connection: iec61850_python.Connection, error: iec61850_python.IedClientError) -> None:
            if is_error(error):
                callback(False, self, custom_id)
                return
            self.set_value(value)
            callback(True, self, custom_id)
            return

        invoke_id, mms_error = model.connection.write_object_async(
            self.get_attribute_reference(),
            self.get_library_functional_constraint(),
            mms_value.lib_object,
            _callback
        )
        if is_error(mms_error):
            print(f"{mms_error=}", flush=True)
            # callback(False, self, custom_id)
            return False
        return True

    def get_controllable_object(self) -> Optional['IEC61850DataObject']:
        if self.can_operate():
            return self.get_parent_data_object()
        return None

    def get_last_value(self) -> Any:
        return self._last_value

    def set_value(self, value: Any):
        old_value = self._last_value
        self._last_value = value
        self._trigger_on_update(old_value, value)
        if old_value != value:
            self._trigger_on_change(old_value, value)

    def add_on_change_callback(self, callback: Callable[['IEC61850RemoteDataAttribute', Any, Any], None]):
        self._on_change_callbacks.append(callback)

    def add_on_update_callback(self, callback: Callable[['IEC61850RemoteDataAttribute', Any, Any], None]):
        self._on_update_callbacks.append(callback)

    def remove_on_change_callback(self, callback):
        if callback in self._on_change_callbacks:
            self._on_change_callbacks.remove(callback)

    def get_protocol_data(self) -> dict:
        return {
            "mms_path": self.get_attribute_reference(),
            "mms_protocol_path": self.get_mms_path(),
            "server": self.get_model().get_server_id(),
            "model": self.get_model().name,
            "logical_device": self.get_logical_device().name,
            "logical_node": self.get_logical_node().name,
            "parents": [parent.name for parent in self.get_parent_objects_and_attributes()],
            "value_type": self.mms_type.name
        }

    def remove_on_update_callback(self, callback):
        if callback in self._on_update_callbacks:
            self._on_update_callbacks.remove(callback)

    def _trigger_on_update(self, old_value: Any, value: Any):
        for callback in self._on_update_callbacks:
            try:
                callback(self, old_value, value)
            except Exception:
                pass

    def _trigger_on_change(self, old_value: Any, value: Any):
        for callback in self._on_change_callbacks:
            try:
                callback(self, old_value, value)
            except Exception:
                pass

    def add_child(self, child: 'IEC61850RemoteDataAttribute') -> bool:
        return self.add_data_attribute(child)

    def add_data_attribute(self, data_attribute: 'IEC61850RemoteDataAttribute') -> bool:
        return super().add_data_attribute(data_attribute)

    def has_data_attribute(self, data_attribute: Union[str, 'IEC61850DataAttribute']) -> bool:
        return super().has_data_attribute(data_attribute)

    def get_data_attribute(self, data_attribute: Union[str, 'IEC61850DataAttribute']) -> 'IEC61850DataAttribute':
        return super().get_data_attribute(data_attribute)

    def has_child(self, child: Union[str, 'IEC61850DataAttribute']) -> bool:
        return self.has_data_attribute(child)

    def ensure_data_attributes(
            self,
            data_attribute_names: List[str],
            data_attribute_type: iec61850_python.DataAttributeType,
            functional_constraint: MMSFunctionalConstraints,
            trigger_options: List[MMSTriggerOptions],
            array_elements: int = 0,
            short_address: int = 0
        ) -> 'IEC61850DataAttribute':
        raise RuntimeError("Cannot ensure data attributes for RemoteDataAttribute")

    def ensure_remote_data_attributes(
            self,
            data_attribute_names: List[str],
            mms_type: iec61850_python.MmsType,
            functional_constraint: MMSFunctionalConstraints) -> 'IEC61850RemoteDataAttribute':

        if len(data_attribute_names) == 0:
            return self
        parent_attribute_name = data_attribute_names[0]
        if self.has_data_attribute(parent_attribute_name):
            parent = self.get_data_attribute(parent_attribute_name)
        else:
            if len(data_attribute_names) > 1:
                raise RuntimeError("Parent data attribute not found - will not create parents without ensuring correct options")

            parent = IEC61850RemoteDataAttribute(
                name=parent_attribute_name,
                mms_type=mms_type,
                functional_constraint=functional_constraint
            )
            self.add_data_attribute(parent)
        return parent.ensure_remote_data_attributes(
            data_attribute_names[1:],
            mms_type=mms_type,
            functional_constraint=functional_constraint
        )
