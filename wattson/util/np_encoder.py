import json

import numpy as np
from powerowl.layers.powergrid.values.grid_value_type import Step


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, np.bool_):
            return bool(obj)
        if isinstance(obj, Step):
            return obj.value
        if hasattr(obj, 'to_dict'):
            return obj.to_dict()
        try:
            return super(NpEncoder, self).default(obj)
        except Exception as e:
            raise ValueError(repr(obj))
