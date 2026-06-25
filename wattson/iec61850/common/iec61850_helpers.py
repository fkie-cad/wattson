from powerowl.layers.network.configuration.protocols.iec61850.mms_functional_constraints import MMSFunctionalConstraints


def attribute_identifier_from_protocol_data(protocol_data: dict) -> str:
    model_name = protocol_data["model"]
    logical_device_name = protocol_data["logical_device"]
    logical_node_name = protocol_data["logical_node"]
    functional_constraint_name = protocol_data["functional_constraint"]
    data_object_name = protocol_data["data_object"]
    data_attribute_name = protocol_data["data_attribute"]

    return model_name + logical_device_name + "/" + logical_node_name + "." + data_object_name + "." + data_attribute_name


def is_error(code) -> bool:
    import iec61850_python
    return code != iec61850_python.IedClientError.IED_ERROR_OK


def parse_variable(variable_name) -> dict:
    parts = variable_name.split("$")
    functional_constraint = MMSFunctionalConstraints(parts[0])
    results = {
        "is_report": False,
        "is_report_attribute": False,
        "is_data_object": False,
        "is_data_attribute": False,
        "is_data_set": False,
        "report_name": None,
        "report_attribute_name": None,
        "object_name": None,
        "parent_attributes": [],
        "attribute_name": None,
        "is_class": False,
        "mms_path": variable_name,
        "functional_constraint": functional_constraint,
    }
    if len(parts) == 1:
        results["is_class"] = True

    if functional_constraint in (MMSFunctionalConstraints.BUFFERED_REPORT, MMSFunctionalConstraints.UNBUFFERED_REPORT):
        if len(parts) > 1:
            results["is_report"] = True
            results["report_name"] = parts[1]
            if len(parts) == 2:
                results["is_report_attribute"] = False
            else:
                results["is_report_attribute"] = True
                results["report_attribute_name"] = parts[2]
    elif functional_constraint in (
        MMSFunctionalConstraints.PROCESS_VALUE_MEASURAND_MX,
        MMSFunctionalConstraints.PROCESS_VALUE_STATUS_ST,
        MMSFunctionalConstraints.PROCESS_COMMAND_ANALOG_SP,
        MMSFunctionalConstraints.PROCESS_COMMAND_BINARY_CO,
        MMSFunctionalConstraints.CONFIGURATION_CF
    ):
        if len(parts) > 1:
            results["object_name"] = parts[1]
            if len(parts) == 2:
                results["is_data_object"] = True
            elif len(parts) >= 3:
                results["is_data_attribute"] = True
                results["attribute_name"] = parts[-1]
                results["parent_attributes"] = parts[2:-1]

    return results
