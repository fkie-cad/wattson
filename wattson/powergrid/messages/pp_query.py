import collections

"""
PPQuery stands for "pandapower query". It is used to interact with the power grid
simulation (implemented using pandapower).
"""
PPQuery = collections.namedtuple("PPQuery", ["table", "column", "index", "value", "log_worthy", "node_id"], defaults=[False, True])
