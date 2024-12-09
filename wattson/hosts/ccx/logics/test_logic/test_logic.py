from wattson.hosts.ccx.logics.ccx_logic import CCXLogic
from wattson.hosts.ccx.logics.logic_return_action import LogicReturnAction


class TestLogic(CCXLogic):
    def __init__(self, ccx, **kwargs):
        super().__init__(ccx, **kwargs)
        self.logger.info("\nTest Logic init\n")

    def start(self):
        self.logger.warning("\nTest Logic start\n")

    def stop(self):
        self.logger.warning("\nTest Logic stop\n")

    def apply(self, event_type: str, args) -> LogicReturnAction:
        self.logger.warning(f"\nTest Logic Apply for {event_type}\n")
        return LogicReturnAction("none")
