class ConfigurationStore(dict):
    """
    Stores (global) configurations accessible by all nodes
    """
    def __init__(self):
        super().__init__()
        self["short-notations"] = {}
        self.short_notations = self["short-notations"]
        self._define_default_callbacks()

    def get_configuration(self, key: str, default_value=None):
        if not key.startswith("!"):
            key = f"!{key}"
        return self.get(key, default_value)

    def register_configuration(self, key: str, value):
        if not key.startswith("!"):
            key = f"!{key}"
        self[key] = value

    def register_short_notation(self, short_key: str, long_key: str):
        if not short_key.startswith("!"):
            short_key = f"!{short_key}"
        if not long_key.startswith("!"):
            long_key = f"!{long_key}"
        self["short-notations"][short_key] = long_key

    def _define_default_callbacks(self):
        self.register_configuration("nodeid", lambda node, store: node.id)
        self.register_configuration("entityid", lambda node, store: node.entity_id)
