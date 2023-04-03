import logging


class BasicLogger(logging.Logger):
    def __init__(self, name: str, fake_logger: bool = False):
        super().__init__(name)
        self.fake = fake_logger

    def add_contexts(self, contexts):
        pass

    def _log(
            self,
            level,
            msg,
            args,
            exc_info=None,
            extra=None,
            stack_info: bool = False,
            stacklevel: int = 1,
            **kwargs
        ) -> None:
        if not self.fake:
            super()._log(level, msg, args, exc_info, extra, stack_info, stacklevel)

    def getChild(self, suffix: str):
        child = super().getChild(suffix)
        child.fake = self.fake
        return child


