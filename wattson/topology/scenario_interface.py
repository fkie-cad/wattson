from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional


class ScenarioInterface(ABC):
    def __init__(self, logger):
        self.logger = logger

    @abstractmethod
    def get_arguments(self) -> dict:
        ...

    @abstractmethod
    def prepare(self, params: dict) -> Optional[Path]:
        ...

    def _check_params(self, params: dict) -> bool:
        args = self.get_arguments()
        for arg, definition in args.items():
            req = definition[0] == "required"
            a_type = definition[1]
            default_value = definition[2]
            if req and arg not in params:
                self.logger.error(f"Attribute {arg} is required")
                return False
            if arg not in params:
                self.logger.debug(f"Setting attribute {arg} to default value {default_value}")
                params[arg] = default_value
            else:
                if type(params[arg]) != a_type:
                    try:
                        if a_type == bool:
                            if params[arg] in ["True", "true", "1"]:
                                params[arg] = True
                            else:
                                params[arg] = False
                        else:
                            v = a_type(params[arg])
                            params[arg] = v
                    except Exception as e:
                        self.logger.error(f"Could not cast {arg} to type {a_type}: {e}")
                        return False
        return True
