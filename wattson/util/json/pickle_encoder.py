import codecs
import json
import pickle

import numpy as np

from wattson.iec104.interface.types import Step


class PickleEncoder(json.JSONEncoder):
    def encode(self, obj):
        # Fix object to add information on key types
        return super(PickleEncoder, self).encode(self._add_key_type_information(obj))

    def iterencode(self, o, _one_shot=False):
        return super(PickleEncoder, self).iterencode(self._add_key_type_information(o), _one_shot=_one_shot)

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

        try:
            if isinstance(obj, dict):
                key_map = {}
                for key in obj.keys():
                    if key is not str:
                        key_map[str(key)] = key
                if len(key_map) > 0:
                    obj["__wattson.keys__"] = key_map

            return super().default(obj)
        except TypeError as e:
            # Use base64 pickle
            try:
                return {
                    "__wattson.type_error__": repr(e),
                    "__wattson.pickled__": True,
                    "__wattson.data__": codecs.encode(pickle.dumps(obj), "base64").decode()
                }
            except AttributeError:
                return {
                    "__wattson.type_error__": repr(e),
                    "__wattson.pickled__": False
                }

    def _add_key_type_information(self, obj):
        if isinstance(obj, list):
            list_copy = []
            for entry in obj:
                list_copy.append(self._add_key_type_information(entry))
            return list_copy

        if isinstance(obj, dict):
            key_map = {}
            typed_obj = {}
            for key, value in obj.items():
                if not isinstance(key, str):
                    key_map[str(key)] = key
                typed_obj[key] = self._add_key_type_information(value)
                if len(key_map) > 0:
                    typed_obj["__wattson.keys__"] = key_map
            return typed_obj

        return obj
