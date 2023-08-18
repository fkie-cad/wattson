import abc
import threading
from abc import ABC, abstractmethod

from typing import TYPE_CHECKING, Optional, Dict

if TYPE_CHECKING:
    from wattson.apps.script_controller import ScriptControllerApp


class Script(ABC):
    def __init__(self, controller: 'ScriptControllerApp', config: Optional[Dict] = None):
        self.controller = controller
        self.config = config if config is not None else {}
        self._termination_event = threading.Event()

    def set_termination_event(self, event: threading.Event):
        self._termination_event = event

    def stop(self):
        pass
