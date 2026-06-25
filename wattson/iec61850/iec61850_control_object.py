import threading

import iec61850_python
from typing import TYPE_CHECKING, Any, Callable, Optional, Union

from wattson.iec61850.common.iec61850_helpers import is_error
from wattson.iec61850.common.mms_control_error import MmsControlError
from wattson.iec61850.common.mms_error import MmsError
from wattson.iec61850.iec61850_mms_value import IEC61850MMSValue
from wattson.util import get_logger

if TYPE_CHECKING:
    from wattson.iec61850.iec61850_data_object import IEC61850DataObject
    from wattson.iec61850.iec61850_remote_data_attribute import IEC61850RemoteDataAttribute


class IEC61850ControlObject:
    def __init__(self, data_object: 'IEC61850DataObject'):
        self._data_object = data_object
        self._control_object = iec61850_python.ControlObject(self._data_object.get_mms_reference(), self._data_object.get_model().connection)
        self.logger = get_logger(f"ControlObject-{self._data_object.get_mms_path()}")

    def get_operate_mms_type(self) -> iec61850_python.MmsType:
        attribute = self.get_operate_mms_attribute()
        if attribute is None:
            raise MmsError(f"No Oper.ctlVal found for {self._data_object.get_mms_path()}")
        return attribute.mms_type

    def get_operate_mms_attribute(self) -> Optional['IEC61850RemoteDataAttribute']:
        path = ["Oper", "ctlVal"]
        if self._data_object.has_data_attribute_path(path):
            data_attribute = self._data_object.get_data_attribute_by_path(path)
            if data_attribute is None:
                return None
            if data_attribute.has_child("f"):
                return data_attribute.get_child("f")
            if data_attribute.has_child("i"):
                return data_attribute.get_child("i")
            return data_attribute
        return None

    def get_control_model(self) -> iec61850_python.ControlModel:
        return self._control_object.get_control_model()

    def operate(self, value: Any) -> bool:
        event = threading.Event()
        success: bool = False

        def callback(_success, _object, _custom_id):
            nonlocal success
            success = True
            event.set()

        if not self.async_operate(value, callback):
            return False
        event.wait()
        return success

    def select(self) -> bool:
        event = threading.Event()
        success: bool = False

        def callback(_success, _object, _custom_id):
            nonlocal success
            success = True
            event.set()

        try:
            if not self.async_select(callback):
                return False
        except:
            success = False
            event.set()
        event.wait()
        return success

    def select_with_value(self, value: Any) -> bool:
        event = threading.Event()
        success: bool = False

        def callback(_success, _object, _custom_id):
            nonlocal success
            success = True
            event.set()

        try:
            if not self.async_select_with_value(value, callback):
                return False
        except:
            success = False
            event.set()
        event.wait()
        return success

    def cancel(self) -> bool:
        event = threading.Event()
        success: bool = False

        def callback(_success, _object, _custom_id):
            nonlocal success
            success = True
            event.set()

        try:
            if not self.async_cancel(callback):
                return False
        except:
            success = False
            event.set()
        event.wait()
        return success

    def select_and_operate(self, value: Any) -> MmsControlError:
        """
        Automatically selects the object before operating if applicable (i.e., when in SBO mode).
        Otherwise, directly operates.

        Args:
            value (Any):
                The target value

        Returns:
            MmsControlError: Whether the operation was successful.
        """
        event = threading.Event()
        mms_control_error = MmsControlError.UNKNOWN_ERROR

        def callback(_success, _object, _custom_id, _mms_control_error):
            nonlocal mms_control_error
            mms_control_error = _mms_control_error
            event.set()

        try:
            self.async_select_and_operate(value, callback, None, self._data_object)
        except:
            mms_control_error = MmsControlError.UNKNOWN_ERROR
            event.set()

        event.wait()
        return mms_control_error

    def async_select_and_operate(self,
                                 value: Any,
                                 callback: Callable[[bool, Union['IEC61850RemoteDataAttribute', 'IEC61850DataObject'], Optional[str], MmsControlError], None],
                                 operation_timestamp: int = 0,
                                 custom_id: Optional[str] = None,
                                 custom_data_object_or_attribute: Optional[Union['IEC61850RemoteDataAttribute', 'IEC61850DataObject']] = None
                                 ) -> MmsControlError:
        """
        Automatically selects the object before operating if applicable (i.e., when in SBO mode).
        Otherwise, directly operates.

        Args:
            value (Any):
                The target value
            callback (Callable[[bool, Union['IEC61850RemoteDataAttribute', 'IEC61850DataObject'], Optional[str], MmsControlError], None]):
                The callback to call after the operation concluded.
            operation_timestamp (int, optional):
                
                (Default value = 0)
            custom_id (Optional[str], optional):
                An optional custom ID to pass to the callback
                (Default value = None)
            custom_data_object_or_attribute (Optional[Union['IEC61850RemoteDataAttribute', 'IEC61850DataObject']], optional):
                An optional custom data object or data attribute to pass to the callback. Per default, this object is used.

        Returns:
            MmsControlError: Whether the request has been made successfully (not whether it was successful!)
        """

        control_model = self.get_control_model()

        if control_model in [iec61850_python.ControlModel.STATUS_ONLY]:
            # callback(False, custom_data_object_or_attribute, custom_id, MmsControlError.STATUS_ONLY)
            return MmsControlError.STATUS_ONLY

        # Direct operation
        if control_model in [iec61850_python.ControlModel.DIRECT_NORMAL, iec61850_python.ControlModel.DIRECT_ENHANCED]:
            # Operate directly
            def _callback(_success, _object, _custom_id):
                mms_control_error = MmsControlError.NO_ERROR
                if not _success:
                    mms_control_error = MmsControlError.OPERATE_ERROR
                callback(_success, _object, _custom_id, mms_control_error)

            if not self.async_operate(value, _callback, operation_timestamp, custom_id, custom_data_object_or_attribute):
                return MmsControlError.OPERATE_ERROR
            return MmsControlError.NO_ERROR


        # First select, then operate
        if control_model in [iec61850_python.ControlModel.SBO_NORMAL, iec61850_python.ControlModel.SBO_ENHANCED]:
            def _operate_callback(_success, _object, _custom_id):
                _mms_control_error = MmsControlError.NO_ERROR
                if not _success:
                    _mms_control_error = MmsControlError.OPERATE_ERROR
                callback(_success, _object, _custom_id, _mms_control_error)

            def _select_callback(_success, _object, _custom_id):
                if not _success:
                    callback(False, custom_data_object_or_attribute, custom_id, MmsControlError.SELECT_ERROR)
                    return
                # Selection was successful -> Operate
                if not self.async_operate(value, _operate_callback, operation_timestamp):
                    callback(False, custom_data_object_or_attribute, custom_id, MmsControlError.OPERATE_ERROR)

            if not self.async_select(_select_callback):
                return MmsControlError.SELECT_ERROR
            return MmsControlError.NO_ERROR

        return MmsControlError.UNKNOWN_ERROR

    def async_operate(self,
                      value: Any,
                      callback: Callable[[bool, Union['IEC61850RemoteDataAttribute', 'IEC61850DataObject'], Optional[str]], None],
                      operation_timestamp: int = 0,
                      custom_id: Optional[str] = None,
                      custom_data_object_or_attribute: Optional[Union['IEC61850RemoteDataAttribute', 'IEC61850DataObject']] = None
                      ) -> bool:
        if custom_data_object_or_attribute is None:
            custom_data_object_or_attribute = self
        if not self._data_object.get_model().is_remote():
            raise RuntimeError("Cannot write value to server without connection")
        try:
            operate_attribute = self.get_operate_mms_attribute()
            if operate_attribute is None:
                raise ValueError("No Operate attribute found")
            mms_value = IEC61850MMSValue.from_mms_value_type(value, operate_attribute.get_mms_value_type(), operate_attribute.get_mms_integer_size())
        except ValueError as e:
            self.logger.error(f"{e=}")
            # callback(False, custom_data_object_or_attribute, custom_id)
            return False
        except MmsError as e:
            self.logger.error(f"{e=}")
            # callback(False, custom_data_object_or_attribute, custom_id)
            return False

        def _callback(control_object: iec61850_python.ControlObject, control_action_type: iec61850_python.ControlActionType,
                      error: iec61850_python.IedClientError, success: bool) -> None:
            callback(success, control_object, custom_id)

        # Operate
        invoke_id, invoke_error = self._control_object.operate_async(mms_value.lib_object, _callback, operation_timestamp)
        if is_error(invoke_error):
            print(f"InvokeError: {invoke_error=}")
            # callback(False, custom_data_object_or_attribute, custom_id)
        return not is_error(invoke_error)

    def async_select(self,
                     callback: Callable[[bool, Union['IEC61850RemoteDataAttribute', 'IEC61850DataObject'], Optional[str]], None],
                     custom_id: Optional[str] = None,
                     custom_data_object_or_attribute: Optional[Union['IEC61850RemoteDataAttribute', 'IEC61850DataObject']] = None
                     ) -> bool:
        if custom_data_object_or_attribute is None:
            custom_data_object_or_attribute = self

        def _callback(control_object: iec61850_python.ControlObject, control_action_type: iec61850_python.ControlActionType,
                      error: iec61850_python.IedClientError, success: bool) -> None:
            callback(success, custom_data_object_or_attribute, custom_id)

        # Select
        invoke_id, invoke_error = self._control_object.select_async(_callback)
        if is_error(invoke_error):
            print(f"InvokeError: {invoke_error=}")
            # callback(False, custom_data_object_or_attribute, custom_id)
        return not is_error(invoke_error)

    def async_select_with_value(self,
                                value: Any,
                                callback: Callable[[bool, Union['IEC61850RemoteDataAttribute', 'IEC61850DataObject'], Optional[str]], None],
                                custom_id: Optional[str] = None,
                                custom_data_object_or_attribute: Optional[Union['IEC61850RemoteDataAttribute', 'IEC61850DataObject']] = None
                                ) -> bool:
        if custom_data_object_or_attribute is None:
            custom_data_object_or_attribute = self
        if not self._data_object.get_model().is_remote():
            raise RuntimeError("Cannot write value to server without connection")
        try:
            operate_attribute = self.get_operate_mms_attribute()
            if operate_attribute is None:
                raise ValueError("No Operate attribute found")
            mms_value = IEC61850MMSValue.from_mms_value_type(value, operate_attribute.get_mms_value_type(), operate_attribute.get_mms_integer_size())
        except ValueError as e:
            # callback(False, custom_data_object_or_attribute, custom_id)
            return False
        except MmsError as e:
            # callback(False, custom_data_object_or_attribute, custom_id)
            return False

        def _callback(control_object: iec61850_python.ControlObject, control_action_type: iec61850_python.ControlActionType,
                      error: iec61850_python.IedClientError, success: bool) -> None:
            callback(success, custom_data_object_or_attribute, custom_id)

        # Select with value
        invoke_id, invoke_error = self._control_object.select_with_value_async(mms_value.lib_object, _callback)
        if is_error(invoke_error):
            print(f"InvokeError: {invoke_error=}")
            # callback(False, custom_data_object_or_attribute, custom_id)
        return not is_error(invoke_error)

    def async_cancel(self,
                     callback: Callable[[bool, Union['IEC61850RemoteDataAttribute', 'IEC61850DataObject'], Optional[str]], None],
                     custom_id: Optional[str] = None,
                     custom_data_object_or_attribute: Optional[Union['IEC61850RemoteDataAttribute', 'IEC61850DataObject']] = None
                     ) -> bool:
        if custom_data_object_or_attribute is None:
            custom_data_object_or_attribute = self

        def _callback(control_object: iec61850_python.ControlObject, control_action_type: iec61850_python.ControlActionType,
                      error: iec61850_python.IedClientError, success: bool) -> None:
            callback(success, custom_data_object_or_attribute, custom_id)

        # Cancel
        invoke_id, invoke_error = self._control_object.cancel_async(_callback)
        if is_error(invoke_error):
            print(f"InvokeError: {invoke_error=}")
            # callback(False, custom_data_object_or_attribute, custom_id)
        return not is_error(invoke_error)
