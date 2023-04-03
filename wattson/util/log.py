import logging
import os
import sys
from typing import Iterable, Optional, Union

from wattson.util.async_logger import AsyncLogger
from wattson.util.basic_logger import BasicLogger
from wattson.util.context_logger import ContextLogger

format = '%(asctime)s - %(name)s - %(levelname)s - %(thread)d.%(process)d -  %(message)s'


def _within_xterm() -> bool:
    return "WINDOWID" in os.environ


def get_logger(host_name: str, logger_name: str, level: int = logging.INFO,
               active_contexts: Optional[Iterable[str]] = None, use_context_logger: bool = False,
               use_basic_logger: bool = False, use_async_logger: bool = True, use_fake_logger: bool = False
               ) -> Union[logging.Logger, ContextLogger, AsyncLogger, BasicLogger]:
    """
    Overwrites logging.get_logger to make it compatible with the ContextLogger

    Args:
        host_name: Host the logger is attaching to
        logger_name: more specific log name
        level: logging-level (INFO)
        active_contexts: contexts enabled by default (for context-logger)
        use_basic_logger: True to use BasicLogger as the wrapper
        use_context_logger: True to use a ContextLogger
        use_async_logger: True to utilize an AsyncLogger that fixes Deadlocks

    Returns:
        new ContextLogger/logging.Logger
    """
    if use_basic_logger:
        logging.setLoggerClass(BasicLogger)
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    logger.propagate = False
    if not logger.hasHandlers():
        formatter = logging.Formatter(format)
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(formatter)
        logger.addHandler(h)

    if use_basic_logger:
        if use_fake_logger:
            logger.fake = True
        return logger

    if use_context_logger:
        logger = ContextLogger(host_name, logger_name, False, format, level, active_contexts, logger=logger)

    if use_async_logger:
        logger = AsyncLogger(logger_name, logger)
    return logger

