import codecs
import json
import pickle


class PickleDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, dct):
        if "__wattson.pickled__" in dct and dct["__wattson.pickled__"] is True:
            dct = pickle.loads(codecs.decode(dct["__wattson.data__"].encode(), "base64"))
        if "__wattson.keys__" in dct:
            decoded_dct = {}
            for key, value in dct.items():
                if key == "__wattson.keys__":
                    continue
                if key in dct["__wattson.keys__"]:
                    decoded_dct[dct["__wattson.keys__"][key]] = value
                else:
                    decoded_dct[key] = value
            dct = decoded_dct
        return dct
