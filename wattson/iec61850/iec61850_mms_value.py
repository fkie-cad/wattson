import iec61850_python
from typing import TYPE_CHECKING, Optional, Any

if TYPE_CHECKING:
    from wattson.iec61850.iec61850_data_attribute import IEC61850DataAttribute


class IEC61850MMSValue:
    def __init__(self, lib_object: iec61850_python.MmsValue, attribute: Optional['IEC61850DataAttribute'] = None):
        self.lib_object = lib_object
        self.data_attribute = attribute

    @property
    def value(self):
        return self.lib_object.get()

    @property
    def name(self):
        if self.data_attribute is None:
            return "?"
        return self.data_attribute.name

    def get_type(self) -> iec61850_python.MmsType:
        return self.lib_object.get_type()

    def is_data_access_error(self) -> bool:
        return self.get_type() == iec61850_python.MmsType.MMS_DATA_ACCESS_ERROR

    @staticmethod
    def from_mms_value_type(value: Any, value_type: iec61850_python.MmsType, integer_size: int = 64) -> 'IEC61850MMSValue':
        lib_object = None
        match value_type:
            case iec61850_python.MmsType.MMS_ARRAY:
                # lib_object = iec61850_python.MmsArray(value)
                pass
            case iec61850_python.MmsType.MMS_STRUCTURE:
                pass
            case iec61850_python.MmsType.MMS_BOOLEAN:
                lib_object = iec61850_python.MmsBoolean(bool(value))
            case iec61850_python.MmsType.MMS_BIT_STRING:
                pass
            case iec61850_python.MmsType.MMS_INTEGER:
                lib_object = iec61850_python.MmsInteger(int(value), integer_size)
            case iec61850_python.MmsType.MMS_UNSIGNED:
                lib_object = iec61850_python.MmsUnsigned(abs(int(value)))
            case iec61850_python.MmsType.MMS_FLOAT:
                # TODO: Is double always the better choice?!
                lib_object = iec61850_python.MmsFloat(float(value), False)
            case iec61850_python.MmsType.MMS_OCTET_STRING:
                pass
            case iec61850_python.MmsType.MMS_VISIBLE_STRING:
                lib_object = iec61850_python.MmsVisibleString.create(str(value))
            case iec61850_python.MmsType.MMS_GENERALIZED_TIME:
                pass
            case iec61850_python.MmsType.MMS_BINARY_TIME:
                pass
            case iec61850_python.MmsType.MMS_BCD:
                pass
            case iec61850_python.MmsType.MMS_OBJ_ID:
                pass
            case iec61850_python.MmsType.MMS_STRING:
                lib_object = iec61850_python.MmsString.create(str(value))
            case iec61850_python.MmsType.MMS_UTC_TIME:
                pass
            case iec61850_python.MmsType.MMS_DATA_ACCESS_ERROR:
                pass
            case _:
                raise ValueError(f"Unsupported MMS value type {value_type.name}")
        if lib_object is None:
            raise ValueError(f"Cannot create MMS value for type {value_type.name}")
        return IEC61850MMSValue(lib_object=lib_object)
