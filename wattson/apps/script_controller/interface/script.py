from abc import ABC, abstractmethod

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from wattson.apps.script_controller import ScriptControllerApp


class Script(ABC):
    def __init__(self, controller: 'ScriptControllerApp'):
        self.controller = controller
