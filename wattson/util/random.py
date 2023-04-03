from numpy.random import RandomState
from hashlib import sha256


class Random:
    _base_seed = 0
    _seed_giver = None
    _instances = {}
    _logger = None

    @staticmethod
    def get_instance(namespace: str) -> RandomState:
        if namespace not in Random._instances:
            if Random._base_seed == 0:
                Random.logger().warning(f"No Base Seed has been set! Using {Random._base_seed}")
            seed = (Random._base_seed + Random.hash(namespace)) & 0xffffffff
            Random.logger().info(f"Creating Random generator for namespace {namespace} with seed {seed}")
            Random._instances[namespace] = RandomState(seed=seed)
        return Random._instances[namespace]

    @staticmethod
    def logger():
        if Random._logger is None:
            from wattson.util.log import get_logger
            Random._logger = get_logger("Wattson Random", "Wattson Random")
        return Random._logger

    @staticmethod
    def normal(base, scale, size=None, ns: str = "default"):
        inst = Random.get_instance(ns)
        return inst.normal(base, scale, size)

    @staticmethod
    def float(lowest, highest, ns: str = "default"):
        inst = Random.get_instance(ns)
        draw = inst.random()
        diff = highest - lowest
        return lowest + draw * diff

    @staticmethod
    def hash(value) -> int:
        return int(sha256(str(value).encode('utf-8')).hexdigest(), 16)

    @staticmethod
    def reset_generators():
        Random._instances = {}

    @staticmethod
    def set_base_seed(seed):
        if Random._base_seed is not None:
            Random.logger().warning(f"Overwriting existing seed with {seed}")
        Random._base_seed = Random.hash(seed)
