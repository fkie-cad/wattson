from dataclasses import dataclass
from typing import Dict, Union

from wattson.util import MissingArgumentError


@dataclass
class BaseHostConfig:
    hostname: str
    ip: str

    @staticmethod
    def from_dict(d: Dict[str, str]) -> 'BaseHostConfig':
        id = d['hostname'] if 'hostname' in d else d['nodeid']
        return BaseHostConfig(id, d['ip'])


class BaseHostInterface:
    def __init__(self, config: Union[Dict, BaseHostConfig]):
        if isinstance(config, dict):
            config = BaseHostConfig.from_dict(config)
        self._config = config

    @property
    def hostname(self):
        return self._config.hostname

    @property
    def ip(self):
        return self._config.ip

'''
class BaseRTUConfig(BaseHostConfig):
    def __init__(self, hostname: str, ip: str, coa: Union[int, str]):
        super().__init__(hostname, ip)
        self.coa = int(coa)

    @staticmethod
    def from_dict(d: Dict[str, Union[str, int]]) -> 'BaseRTUConfig':
        id = d['hostname'] if 'hostname' in d else d['nodeid']
        return BaseRTUConfig(id, d['ip'], d['coa'])


class BaseRTUInterface(BaseHostInterface):
    def __init__(self, config: BaseRTUConfig):
        self._config = config

    @property
    def coa(self):
        return self._config.coa
'''