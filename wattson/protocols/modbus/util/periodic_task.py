import asyncio
import logging
import traceback
from contextlib import suppress
from typing import Callable, Optional

from wattson.util import get_logger


class PeriodicTask:
    """
    A PeriodicTask should be run in a dedicated thread with an existing asyncio event loop.
    With the given periodicity, the respective callback will be called. For each call, the provided arguments are passed.
    """
    def __init__(self, period_seconds: float, callback: Callable, callback_args: list, logger: Optional[logging.Logger] = None):
        self.period_seconds = period_seconds
        self.callback = callback
        self.callback_args = callback_args
        self._task: Optional[asyncio.Task] = None
        self._is_started: bool = False
        self.logger = logger or get_logger("PeriodicTask")

    async def start(self):
        if not self._is_started:
            self._is_started = True
            self._task = asyncio.ensure_future(self._run())

    async def stop(self):
        if self._is_started:
            self._is_started = False
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task

    async def _run(self):
        while True:
            await asyncio.sleep(self.period_seconds)
            try:
                self.callback(*self.callback_args)
            except Exception as e:
                self.logger.debug(e)
