import threading
from typing import List, TYPE_CHECKING, Optional, Union, Any, Callable

import iec61850_python
from pydantic_core.core_schema import float_schema

from powerowl.layers.network.configuration.protocols.iec61850.mms_functional_constraints import MMSFunctionalConstraints
from powerowl.layers.network.configuration.protocols.iec61850.mms_trigger_options import MMSTriggerOptions
from wattson.iec61850.common.iec61850_python_mappings import iec61850_python_mappings
from wattson.iec61850.common.mms_error import MmsError
from wattson.iec61850.iec61850_control_object import IEC61850ControlObject
from wattson.iec61850.iec61850_logical_device import IEC61850LogicalDevice
from wattson.iec61850.iec61850_mms_value import IEC61850MMSValue

if TYPE_CHECKING:
    from wattson.iec61850.iec61850_model import IEC61850Model
    from wattson.iec61850.iec61850_logical_device import IEC61850LogicalDevice
    from wattson.iec61850.iec61850_logical_node import IEC61850LogicalNode
    from wattson.iec61850.iec61850_data_attribute import IEC61850DataAttribute
    from wattson.iec61850.iec61850_remote_data_attribute import IEC61850RemoteDataAttribute


class IEC61850DataObject:
    def __init__(self,
                 lib_object: iec61850_python.DataObject,
                 parent: Optional[Union['IEC61850LogicalNode', 'IEC61850DataObject']] = None,
                 children: Optional[List[Union['IEC61850DataObject', 'IEC61850DataAttribute']]] = None) -> None:

        if children is None:
            children = []
        self.parent = parent
        self.lib_object = lib_object
        self.children = children
        self._control_object: Optional[iec61850_python.ControlObject] = None

    @property
    def name(self) -> str:
        return self.lib_object.get_name()

    def is_remote(self) -> bool:
        return self.get_model().is_remote()

    def set_control_model(self, control_model: iec61850_python.ControlModel) -> None:
        if self.is_remote():
            raise NotImplemented("NIY")
        else:
            self.lib_object.set_control_model(control_model)

    def __hash__(self):
        return hash(id(self))

    def get_mms_path(self) -> str:
        parts = [self.get_logical_node().name]
        parts.extend(parent.name for parent in self.get_parent_objects())
        parts.append(self.name)
        return "$".join(parts)

    def get_mms_reference(self) -> str:
        return f"{self.parent.get_mms_reference()}.{self.name}"

    def get_parent_objects(self) -> List['IEC61850DataObject']:
        from wattson.iec61850.iec61850_logical_node import IEC61850LogicalNode
        if isinstance(self.parent, IEC61850LogicalNode):
            return []
        return self.parent.get_parent_objects() + [self.parent]

    def get_logical_node(self) -> 'IEC61850LogicalNode':
        from wattson.iec61850.iec61850_logical_node import IEC61850LogicalNode
        if isinstance(self.parent, IEC61850LogicalNode):
            return self.parent
        return self.parent.get_logical_node()

    def get_logical_device(self) -> 'IEC61850LogicalDevice':
        return self.get_logical_node().get_logical_device()

    def get_model(self) -> 'IEC61850Model':
        return self.get_logical_node().get_model()

    def add_child(self, child: Union['IEC61850DataObject', 'IEC61850DataAttribute']) -> bool:
        if self.has_child(child):
            return False
        child.parent = self
        self.children.append(child)
        return True

    def add_data_object(self, data_object: 'IEC61850DataObject') -> bool:
        if self.has_data_object(data_object):
            return False
        data_object.parent = self
        self.children.append(data_object)
        return True

    def add_data_attribute(self, data_attribute: 'IEC61850DataAttribute') -> bool:
        if self.has_data_attribute(data_attribute):
            return False
        data_attribute.parent = self
        self.children.append(data_attribute)
        return True

    def has_data_object(self, data_object: Union[str, 'IEC61850DataObject']) -> bool:
        if isinstance(data_object, str):
            return self.has_child(data_object)
        return data_object in self.get_data_object_children()

    def get_child_by_path(self, data_path: List[str]) -> Optional[Union['IEC61850DataObject', 'IEC61850DataAttribute']]:
        data_path = data_path.copy()
        current = self
        while len(data_path) > 0:
            next_child = data_path.pop(0)
            if not current.has_child(next_child):
                return None
            child = current.get_child(next_child)
            if len(data_path) == 0:
                return child
            current = child
        return current

    def get_data_attribute_by_path(self, data_attribute_path: List[str]) -> Optional['IEC61850DataAttribute']:
        from wattson.iec61850.iec61850_data_attribute import IEC61850DataAttribute
        candidate = self.get_child_by_path(data_attribute_path)
        if candidate is None:
            return None
        if not isinstance(candidate, IEC61850DataAttribute):
            return None
        return candidate

    def has_data_attribute_path(self, data_attribute_path: List[str]) -> bool:
        return self.get_data_attribute_by_path(data_attribute_path) is not None

    def has_data_attribute(self, data_attribute: Union[str, 'IEC61850DataAttribute']) -> bool:
        if isinstance(data_attribute, str):
            return self.has_child(data_attribute)
        return data_attribute in self.get_data_attribute_children()

    def get_data_attribute(self, data_attribute: Union[str, 'IEC61850DataAttribute']) -> 'IEC61850DataAttribute':
        data_attribute_name = data_attribute if isinstance(data_attribute, str) else data_attribute.name
        for existing_data_attribute in self.get_data_attribute_children():
            if existing_data_attribute.name == data_attribute_name:
                return existing_data_attribute
        raise KeyError(f"No DataAttribute with name {data_attribute_name} exists at {self.name}")

    def get_data_object(self, data_object: Union[str, 'IEC61850DataObject']) -> 'IEC61850DataObject':
        data_object_name = data_object if isinstance(data_object, str) else data_object.name
        for existing_data_object in self.get_data_object_children():
            if existing_data_object.name == data_object_name and isinstance(existing_data_object, IEC61850DataObject):
                return existing_data_object
        raise KeyError(f"No DataObject with name {data_object_name} exists at {self.name}")

    def get_data_attributes(self) -> List['IEC61850DataAttribute']:
        from wattson.iec61850.iec61850_data_attribute import IEC61850DataAttribute
        attributes = []
        for child in self.children:
            if isinstance(child, IEC61850DataObject):
                attributes.extend(child.get_data_attributes())
            elif isinstance(child, IEC61850DataAttribute):
                attributes.append(child)
                attributes.extend(child.get_data_attributes())
        return attributes

    def get_data_object_children(self) -> List['IEC61850DataObject']:
        return [child for child in self.children if isinstance(child, IEC61850DataObject)]

    def get_data_attribute_children(self) -> List['IEC61850DataAttribute']:
        from wattson.iec61850.iec61850_data_attribute import IEC61850DataAttribute
        return [child for child in self.children if isinstance(child, IEC61850DataAttribute)]

    def get_child(self, child: str) -> Optional[Union['IEC61850DataObject', 'IEC61850DataAttribute']]:
        for existing_child in self.children:
            if child == existing_child.name:
                return existing_child
        return None

    def has_child(self, child: Union[str, 'IEC61850DataObject', 'IEC61850DataAttribute']) -> bool:
        from wattson.iec61850.iec61850_data_attribute import IEC61850DataAttribute
        if isinstance(child, str):
            for existing_child in self.children:
                if existing_child.name == child:
                    return True
        if isinstance(child, IEC61850DataObject):
            return child in self.get_data_object_children()
        if isinstance(child, IEC61850DataAttribute):
            return child in self.get_data_attribute_children()
        return False

    def ensure_data_objects(self, data_object_names: List[str]) -> 'IEC61850DataObject':
        if len(data_object_names) == 0:
            return self
        data_object_name = data_object_names[0]
        if self.has_data_object(data_object_name):
            parent = self.get_data_object(data_object_name)
        else:
            lib_data_object = self.lib_object.add_data_object(data_object_name, 0)
            parent = IEC61850DataObject(lib_data_object)
            self.add_data_object(parent)
        return parent.ensure_data_objects(data_object_names[1:])

    def ensure_data_attributes(
            self,
            data_attribute_names: List[str],
            data_attribute_type: Union[iec61850_python.DataAttributeType, iec61850_python.MmsType],
            functional_constraint: MMSFunctionalConstraints,
            trigger_options: List[MMSTriggerOptions],
            array_elements: int = 0,
            short_address: int = 0
        ) -> 'IEC61850DataAttribute':
        from wattson.iec61850.iec61850_data_attribute import IEC61850DataAttribute

        if len(data_attribute_names) == 0:
            raise ValueError(f"Cannot ensure empty set of attributes for data object {self.name}")
        parent_attribute_name = data_attribute_names[0]
        if self.has_data_attribute(parent_attribute_name):
            parent = self.get_data_attribute(parent_attribute_name)
        else:
            if len(data_attribute_names) > 1:
                raise RuntimeError("Parent data attribute not found - will not create parents without ensuring correct options")

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

    def ensure_remote_data_attributes(
            self, data_attribute_names: List[str], mms_type: iec61850_python.MmsType, functional_constraint: MMSFunctionalConstraints
    ) -> 'IEC61850RemoteDataAttribute':
        from wattson.iec61850.iec61850_remote_data_attribute import IEC61850RemoteDataAttribute

        if len(data_attribute_names) == 0:
            raise ValueError(f"Cannot ensure empty set of attributes for data object {self.name}")
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

    def get_control_object(self) -> IEC61850ControlObject:
        if not self.is_remote():
            raise RuntimeError("ControlObjects are only supported for a remote model")
        if self._control_object is None:
            self._control_object = IEC61850ControlObject(self)
        return self._control_object

