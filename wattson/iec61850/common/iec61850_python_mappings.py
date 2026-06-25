import iec61850_python


class iec61850_python_mappings:
    trigger_options_mapping = {"TRG_OPT_DATA_CHANGED": iec61850_python.TRG_OPT_DATA_CHANGED,
                               "TRG_OPT_QUALITY_CHANGED": iec61850_python.TRG_OPT_QUALITY_CHANGED,
                               "TRG_OPT_DATA_UPDATE": iec61850_python.TRG_OPT_DATA_UPDATE,
                               "TRG_OPT_INTEGRITY": iec61850_python.TRG_OPT_INTEGRITY,
                               "TRG_OPT_GI": iec61850_python.TRG_OPT_GI,
                               "TRG_OPT_TRANSIENT": iec61850_python.TRG_OPT_TRANSIENT}

    inclusion_options_mapping = {"RPT_OPT_SEQ_NUM": iec61850_python.RPT_OPT_SEQ_NUM,
                                 "RPT_OPT_TIME_STAMP": iec61850_python.RPT_OPT_TIME_STAMP,
                                 "RPT_OPT_REASON_FOR_INCLUSION": iec61850_python.RPT_OPT_REASON_FOR_INCLUSION,
                                 "RPT_OPT_DATA_SET": iec61850_python.RPT_OPT_DATA_SET,
                                 "RPT_OPT_DATA_REFERENCE": iec61850_python.RPT_OPT_DATA_REFERENCE,
                                 "RPT_OPT_BUFFER_OVERFLOW": iec61850_python.RPT_OPT_BUFFER_OVERFLOW,
                                 "RPT_OPT_ENTRY_ID": iec61850_python.RPT_OPT_ENTRY_ID,
                                 "RPT_OPT_CONF_REV": iec61850_python.RPT_OPT_CONF_REV}

    functional_constraint_mapping = {key: value for key, value in
                                     iec61850_python.FunctionalConstraint.__members__.items()}

    attribute_type_mapping = {key: value for key, value in
                              iec61850_python.DataAttributeType.__members__.items()}
