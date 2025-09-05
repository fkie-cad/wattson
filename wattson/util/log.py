import logging
import os
import socket
import sys
import syslog
from logging.handlers import SYSLOG_UDP_PORT
from typing import Iterable, Optional, Union, Dict

from wattson.util.async_logger import AsyncLogger
from wattson.util.basic_logger import BasicLogger
from wattson.util.context_logger import ContextLogger

# format = '%(asctime)s - %(name)s - %(levelname)s - %(thread)d.%(process)d - %(message)s'
log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

syslog_format = "[%(name)s][%(process)d]: %(message)s"

# Fix numba logging spam
logging.getLogger('numba').setLevel(logging.WARNING)


def _within_xterm() -> bool:
    return "WINDOWID" in os.environ


def get_syslog_socket():
    # TODO: Detect if this is correct
    return "/dev/log"


def get_logger(host_name: str,
               logger_name: Optional[str] = None,
               level: int = logging.INFO,
               active_contexts: Optional[Iterable[str]] = None,
               use_context_logger: bool = False,
               use_basic_logger: bool = True,
               use_fake_logger: bool = False,
               syslog_config: Union[bool, Dict] = False) -> Union[logging.Logger, ContextLogger, AsyncLogger, BasicLogger]:
    """
    Overwrites logging.get_logger to make it compatible with the ContextLogger

    Args:
        host_name (str):
            Host the logger is attaching to
        logger_name (Optional[str], optional):
            more specific log name
            (Default value = None)
        level (int, optional):
            logging-level (INFO)
            (Default value = logging.INFO)
        active_contexts (Optional[Iterable[str]], optional):
            contexts enabled by default (for context-logger)
        use_context_logger (bool, optional):
            True to use a ContextLogger
            (Default value = False)
        use_basic_logger (bool, optional):
            True to use BasicLogger as the wrapper
            (Default value = True)
        use_fake_logger (bool, optional):
            True to disable actual logging
            (Default value = False)
        syslog_config (Union[bool, Dict], optional):
            A dictionary defining the syslog behavior - address: Tuple[string, int] (Address & Port) (localhost, SYSLOG_UDP_PORT) or socket file
            ("/dev/log") - facility: syslog facility (LOG_DAEMON) - socket_type: socket type (socket.SOCK_DGRAM) If set to True, syslog is enabled
            with default parameters.
            If set to False, syslog is disabled

    Returns:
        new ContextLogger/logging.Logger
    """
    if logger_name is None:
        logger_name = host_name

    if use_basic_logger:
        logging.setLoggerClass(BasicLogger)

    logger = logging.getLogger(logger_name)
    logger.level = level
    logger.propagate = False

    if not logger.hasHandlers():
        formatter = logging.Formatter(log_format)
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(formatter)
        logger.addHandler(h)

        if syslog_config is not False:
            if syslog_config is True:
                syslog_config = {
                    "address": get_syslog_socket(),  # ("localhost", SYSLOG_UDP_PORT),
                    "facility": syslog.LOG_DAEMON,
                    "socket_type": socket.SOCK_DGRAM
                }
            logger.info(f"Enabling syslog logger: {repr(syslog_config)}")
            syslog_handler = logging.handlers.SysLogHandler(
                address=syslog_config["address"],
                facility=syslog_config["facility"],
                socktype=syslog_config["socket_type"]
            )
            syslog_formatter = logging.Formatter(syslog_format)
            syslog_handler.setFormatter(syslog_formatter)
            logger.addHandler(syslog_handler)

    if use_basic_logger:
        if use_fake_logger:
            logger.fake = True
        return logger

    if use_context_logger:
        logger = ContextLogger(host_name, logger_name, False, log_format, level, active_contexts, logger=logger)

    #if use_async_logger:
    #    logger = AsyncLogger(logger_name, logger)
    return logger

