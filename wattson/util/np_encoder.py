import json
from pathlib import Path

import numpy as np
from powerowl.layers.powergrid.values.grid_value_type import Step
from wattson.iec104.interface.types.step import Step as Step104
from c104 import Step as c104Step


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
        if isinstance(obj, Step104):
            return obj.value
        if isinstance(obj, c104Step):
            return obj.value
        if isinstance(obj, set):
            return list(obj)
        if isinstance(obj, Path):
            return str(obj.absolute())
        if hasattr(obj, 'to_dict'):
            return obj.to_dict()
        try:
            return super(NpEncoder, self).default(obj)
        except Exception as e:
            raise ValueError(repr(obj) + f" ({type(obj)})") from e
