from typing import List, TYPE_CHECKING, Optional, Union, Any

import iec61850_python
from pydantic_core.core_schema import float_schema

from powerowl.layers.network.configuration.protocols.iec61850.mms_functional_constraints import MMSFunctionalConstraints
from powerowl.layers.network.configuration.protocols.iec61850.mms_trigger_options import MMSTriggerOptions
from wattson.iec61850.common.iec61850_python_mappings import iec61850_python_mappings
from wattson.iec61850.iec61850_mms_value import IEC61850MMSValue

if TYPE_CHECKING:
    from wattson.iec61850.iec61850_model import IEC61850Model
    from wattson.iec61850.iec61850_logical_device import IEC61850LogicalDevice
    from wattson.iec61850.iec61850_logical_node import IEC61850LogicalNode
    from wattson.iec61850.iec61850_data_object import IEC61850DataObject


class IEC61850DataAttribute:
    def __init__(self,
                 lib_object: iec61850_python.DataAttribute,
                 functional_constraint: MMSFunctionalConstraints,
                 parent: Optional[Union['IEC61850DataObject', 'IEC61850DataAttribute']] = None,
                 children: Optional[List['IEC61850DataAttribute']] = None) -> None:

        if children is None:
            children = []
        self.parent = parent
        self.lib_object = lib_object
        self.mms_type: Optional[iec61850_python.MmsType] = None

        self.children = children
        self.functional_constraint = functional_constraint
        self._data_point_identifier: Optional[str] = None

    @property
    def name(self) -> str:
        return self.lib_object.get_name()

    def is_measurement(self) -> bool:
        return self.functional_constraint in [
            MMSFunctionalConstraints.PROCESS_VALUE_STATUS_ST,
            MMSFunctionalConstraints.PROCESS_VALUE_MEASURAND_MX
        ]

    def is_monitoring(self) -> bool:
        return self.functional_constraint in [
            MMSFunctionalConstraints.CONFIGURATION_CF,
            MMSFunctionalConstraints.DESCRIPTION_DC
        ] or self.is_measurement()

    def get_type(self) -> iec61850_python.DataAttributeType:
        return self.lib_object.get_type()

    def get_mms_value_type(self) -> iec61850_python.MmsType:
        return IEC61850DataAttribute.attribute_type_to_value_type(self.get_type())

    def get_mms_integer_size(self) -> int:
        integer_size = IEC61850DataAttribute.attribute_type_to_integer_size(self.get_type())
        if integer_size is None:
            return 64
        return integer_size

    def get_mms_value(self) -> IEC61850MMSValue:
        return IEC61850MMSValue(self.lib_object.get_value(), self)

    @staticmethod
    def attribute_type_to_integer_size(attribute_type: iec61850_python.DataAttributeType) -> Optional[int]:
        if attribute_type == iec61850_python.DataAttributeType.IEC61850_INT8:
            return 8
        if attribute_type == iec61850_python.DataAttributeType.IEC61850_INT16:
            return 16
        if attribute_type == iec61850_python.DataAttributeType.IEC61850_INT32:
            return 32
        if attribute_type == iec61850_python.DataAttributeType.IEC61850_INT64:
            return 64
        if attribute_type == iec61850_python.DataAttributeType.IEC61850_ENUMERATED:
            return 8
        return None

    @staticmethod
    def attribute_type_to_value_type(attribute_type: iec61850_python.DataAttributeType) -> iec61850_python.MmsType:
        # According to https://github.com/mz-automation/libiec61850/blob/v1.6/src/iec61850/server/mms_mapping/mms_mapping.c#L174
        if attribute_type in [iec61850_python.DataAttributeType.IEC61850_BOOLEAN]:
            return iec61850_python.MmsType.MMS_BOOLEAN

        if attribute_type in [
            iec61850_python.DataAttributeType.IEC61850_INT8,
            iec61850_python.DataAttributeType.IEC61850_INT16,
            iec61850_python.DataAttributeType.IEC61850_INT32,
            iec61850_python.DataAttributeType.IEC61850_INT64,
            iec61850_python.DataAttributeType.IEC61850_INT128,
            iec61850_python.DataAttributeType.IEC61850_ENUMERATED
        ]:
            return iec61850_python.MmsType.MMS_INTEGER

        if attribute_type in [
            iec61850_python.DataAttributeType.IEC61850_INT8U,
            iec61850_python.DataAttributeType.IEC61850_INT16U,
            iec61850_python.DataAttributeType.IEC61850_INT24U,
            iec61850_python.DataAttributeType.IEC61850_INT32U
        ]:
            return iec61850_python.MmsType.MMS_UNSIGNED

        if attribute_type in [
            iec61850_python.DataAttributeType.IEC61850_FLOAT32,
            iec61850_python.DataAttributeType.IEC61850_FLOAT64
        ]:
            return iec61850_python.MmsType.MMS_FLOAT

        if attribute_type in [
            iec61850_python.DataAttributeType.IEC61850_CHECK,
            iec61850_python.DataAttributeType.IEC61850_CODEDENUM,
            iec61850_python.DataAttributeType.IEC61850_QUALITY,
            iec61850_python.DataAttributeType.IEC61850_GENERIC_BITSTRING,
            iec61850_python.DataAttributeType.IEC61850_OPTFLDS,
            iec61850_python.DataAttributeType.IEC61850_TRGOPS
        ]:
            return iec61850_python.MmsType.MMS_BIT_STRING

        if attribute_type in [
            iec61850_python.DataAttributeType.IEC61850_OCTET_STRING_6,
            iec61850_python.DataAttributeType.IEC61850_OCTET_STRING_8,
            iec61850_python.DataAttributeType.IEC61850_OCTET_STRING_64
        ]:
            return iec61850_python.MmsType.MMS_OCTET_STRING

        if attribute_type in [
            iec61850_python.DataAttributeType.IEC61850_CURRENCY,
            iec61850_python.DataAttributeType.IEC61850_VISIBLE_STRING_32,
            iec61850_python.DataAttributeType.IEC61850_VISIBLE_STRING_64,
            iec61850_python.DataAttributeType.IEC61850_VISIBLE_STRING_65,
            iec61850_python.DataAttributeType.IEC61850_VISIBLE_STRING_129,
            iec61850_python.DataAttributeType.IEC61850_VISIBLE_STRING_255
        ]:
            return iec61850_python.MmsType.MMS_VISIBLE_STRING

        if attribute_type in [
            iec61850_python.DataAttributeType.IEC61850_UNICODE_STRING_255
        ]:
            return iec61850_python.MmsType.MMS_UNICODE_STRING

        if attribute_type in [
            iec61850_python.DataAttributeType.IEC61850_TIMESTAMP
        ]:
            return iec61850_python.MmsType.MMS_UTC_TIME

        if attribute_type in [
            iec61850_python.DataAttributeType.IEC61850_ENTRY_TIME
        ]:
            return iec61850_python.MmsType.MMS_BINARY_TIME

        if attribute_type in [
            iec61850_python.DataAttributeType.IEC61850_CONSTRUCTED
        ]:
            return iec61850_python.MmsType.MMS_STRUCTURE

        raise AttributeError(f"Unsupported attribute type {attribute_type=}")

    def update_model_value(self, value: Any) -> bool:
        if self.is_control():
            return False
        try:
            with self.get_model().model_lock:
                if not isinstance(value, IEC61850MMSValue):
                    mms_value = IEC61850MMSValue.from_mms_value_type(value, self.get_mms_value_type(), self.get_mms_integer_size())
                else:
                    mms_value = value

                print(f"{self.name} -> {value}: {mms_value.value} ({self.get_mms_value_type()} | {mms_value.get_type()}) | {self.get_type().name}", flush=True)
                self.lib_object.update_value(mms_value.lib_object)
            return True
        except Exception as e:
            raise e
            return False

    def can_operate(self) -> bool:
        return self.is_control()

    def is_control(self) -> bool:
        return self.functional_constraint in [
            MMSFunctionalConstraints.PROCESS_COMMAND_BINARY_CO,
            MMSFunctionalConstraints.PROCESS_COMMAND_ANALOG_SP
        ]

    def is_configuration(self) -> bool:
        return self.functional_constraint in [
            MMSFunctionalConstraints.CONFIGURATION_CF
        ]

    def get_functional_constraint(self) -> MMSFunctionalConstraints:
        return self.functional_constraint

    def get_library_functional_constraint(self) -> iec61850_python.FunctionalConstraint:
        lib_functional_constraint: iec61850_python.FunctionalConstraint = iec61850_python_mappings.functional_constraint_mapping.get(
            "IEC61850_FC_" + self.functional_constraint.value
        )
        return lib_functional_constraint

    def get_parent_data_object(self) -> 'IEC61850DataObject':
        from wattson.iec61850.iec61850_data_object import IEC61850DataObject
        if isinstance(self.parent, IEC61850DataObject):
            return self.parent
        return self.parent.get_parent_data_object()

    def get_mms_path(self) -> str:
        parts = [self.get_logical_node().name, self.get_functional_constraint().value]
        parts.extend(parent.name for parent in self.get_parent_objects_and_attributes())
        parts.append(self.name)
        return "$".join(parts)

    def get_attribute_reference(self) -> str:
        parts = [self.get_logical_node().name]
        parts.extend(parent.name for parent in self.get_parent_objects_and_attributes())
        parts.append(self.name)
        return f'{self.get_logical_device().reference}/{".".join(parts)}'

    def get_parent_objects_and_attributes(self) -> List[Union['IEC61850DataObject', 'IEC61850DataAttribute']]:
        from wattson.iec61850.iec61850_data_object import IEC61850DataObject
        if isinstance(self.parent, IEC61850DataObject):
            return self.parent.get_parent_objects() + [self.parent]
        return self.parent.get_parent_objects_and_attributes() + [self.parent]

    def get_logical_node(self) -> 'IEC61850LogicalNode':
        return self.parent.get_logical_node()

    def get_logical_device(self) -> 'IEC61850LogicalDevice':
        return self.parent.get_logical_device()

    def get_model(self) -> 'IEC61850Model':
        return self.parent.get_model()

    def add_child(self, child: 'IEC61850DataAttribute') -> bool:
        return self.add_data_attribute(child)

    def add_data_attribute(self, data_attribute: 'IEC61850DataAttribute') -> bool:
        if self.has_data_attribute(data_attribute):
            return False
        data_attribute.parent = self
        self.children.append(data_attribute)
        return True

    def has_data_attribute(self, data_attribute: Union[str, 'IEC61850DataAttribute']) -> bool:
        if isinstance(data_attribute, str):
            for child in self.children:
                if child.name == data_attribute:
                    return True
            return False
        return data_attribute in self.children

    def get_data_attribute(self, data_attribute: Union[str, 'IEC61850DataAttribute']) -> 'IEC61850DataAttribute':
        data_attribute_name = data_attribute if isinstance(data_attribute, str) else data_attribute.name
        for child in self.children:
            if child.name == data_attribute_name:
                return child
        raise KeyError(f"DataAttribute {data_attribute_name} does not exist in DataAttribute {self.name}")

    def get_data_attributes(self) -> List['IEC61850DataAttribute']:
        attributes = []
        for child in self.children:
            attributes.append(child)
            attributes.extend(child.get_data_attributes())
        return attributes

    def has_child(self, child: Union[str, 'IEC61850DataAttribute']) -> bool:
        return self.has_data_attribute(child)

    def get_child(self, child: str) -> Optional['IEC61850DataAttribute']:
        for existing_child in self.children:
            if child == existing_child.name:
                return existing_child
        return None

    def ensure_data_attributes(
            self,
            data_attribute_names: List[str],
            data_attribute_type: iec61850_python.DataAttributeType,
            functional_constraint: MMSFunctionalConstraints,
            trigger_options: List[MMSTriggerOptions],
            array_elements: int = 0,
            short_address: int = 0
    ) -> 'IEC61850DataAttribute':

        if len(data_attribute_names) == 0:
            return self
        parent_attribute_name = data_attribute_names[0]
        if self.has_data_attribute(parent_attribute_name):
            parent = self.get_data_attribute(parent_attribute_name)
        else:
            if len(data_attribute_names) > 1:
                raise RuntimeError(f"Parent data attribute {parent_attribute_name} ({data_attribute_names}) not found in {self.get_mms_path()} - will not create parents without ensuring correct options")

            lib_functional_constraint: iec61850_python.FunctionalConstraint = iec61850_python_mappings.functional_constraint_mapping.get(
                "IEC61850_FC_" + functional_constraint.value
            )
            if lib_functional_constraint is None:
                raise KeyError(f"Could not find functional constraint {functional_constraint.value}.")
            lib_trigger_options = 0
            for option in trigger_options:
                option = iec61850_python_mappings.trigger_options_mapping.get(option.value)
                if option is None:
                    raise KeyError(f"Could not find trigger option {option}.")
                lib_trigger_options |= option

            lib_data_attribute = self.lib_object.add_data_attribute(
                parent_attribute_name,
                data_attribute_type,
                lib_functional_constraint,
                lib_trigger_options,
                array_elements,
                short_address
            )
            parent = IEC61850DataAttribute(lib_data_attribute, functional_constraint=functional_constraint)
            self.add_data_attribute(parent)
        return parent.ensure_data_attributes(
            data_attribute_names[1:],
            data_attribute_type=data_attribute_type,
            functional_constraint=functional_constraint,
            trigger_options=trigger_options,
            array_elements=array_elements,
            short_address=short_address
        )

    def link_data_point(self, data_point_identifier: str):
        self._data_point_identifier = data_point_identifier
        self.get_model().register_data_point(data_point_identifier, self)

    @property
    def data_point_identifier(self) -> Optional[str]:
        return self._data_point_identifier
