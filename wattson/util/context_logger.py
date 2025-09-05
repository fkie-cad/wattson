from enum import Enum
import logging
from logging import *
from typing import Any, Optional, Dict, Iterable, Set


class ContextLogger(logging.Logger):
    """Wraps the regular logger, called after as ContextLogger(logging.getLogger(xyz))"""

    def __init__(self, host_name: str, logger_name: str, within_xterm: bool,
                 _format: str, lvl: int = logging.INFO,
                 active_contexts: Optional[Iterable[str]] = None,
                 logger: Optional[Logger] = None):

        self._host_name = host_name
        self._logger_name = logger_name
        self._within_xterm = within_xterm
        self._format = _format
        if logger is None:
            #logging_fname = host_name + "_log"
            logging.basicConfig(format=self._format, level=lvl)
            logger = logging.getLogger(self._logger_name)
            logger.setLevel(lvl)
            logger.propagate = False
            formatter = logging.Formatter(self._format)
            h = logging.StreamHandler()
            h.setFormatter(formatter)
            logger.addHandler(h)
        else:
            logger.setLevel(lvl)
        self.real_logger = logger
        self.active_contexts = set(active_contexts) if active_contexts else set()

        super().__init__(logger_name)

        self.super_log_resolver = {
            "warning": self.real_logger.warning,
            "critical": self.real_logger.critical,
            "info": self.real_logger.info,
            "debug": self.real_logger.debug,
            "error": self.real_logger.error,
            "exception": self.real_logger.exception,
        }

    def add_contexts(self, contexts: Iterable[str]):
        self.active_contexts |= set(contexts)

    def warning(self, msg: Any, *args: Any, exc_info: Any = ..., stack_info: bool = ...,
                stacklevel: int = ..., extra: Optional[Dict[str, Any]] = ...,
                **kwargs: Any) -> None:
        if self.level > logging.WARNING:
            return
        self._handle_msg(msg, log_func="warning", **kwargs)

    def critical(self, msg: Any, *args: Any, exc_info: Any = False, stack_info: bool = False,
                 stacklevel: int = ..., extra: Optional[Dict[str, Any]] = ...,
                 **kwargs: Any) -> None:
        if self.level > logging.CRITICAL:
            return
        kwargs['exc_info'] = exc_info
        kwargs['stack_info'] = stack_info
        self._handle_msg(msg, log_func="critical", **kwargs)

    def info(self, msg: Any, *args: Any, exc_info: Any = ..., stack_info: bool = ...,
             stacklevel: int = ..., extra: Optional[Dict[str, Any]] = ...,
             **kwargs: Any) -> None:
        if self.level > logging.INFO:
            return
        self._handle_msg(msg, log_func="info", **kwargs)

    def debug(self, msg: Any, *args: Any, exc_info: Any = False, stack_info: bool = False,
              stacklevel: int = ..., extra: Optional[Dict[str, Any]] = ...,
              **kwargs: Any) -> None:
        if self.level > logging.DEBUG:
            return
        self._handle_msg(msg, log_func="debug", **kwargs)

    def error(self, msg: Any, *args: Any, exc_info: Any = ..., stack_info: bool = ...,
              stacklevel: int = ..., extra: Optional[Dict[str, Any]] = ...,
              **kwargs: Any) -> None:
        if self.level > logging.ERROR:
            return
        self._handle_msg(msg, log_func="error", **kwargs)

    def exception(self, msg: Any, *args: Any, exc_info: Any = ..., stack_info: bool = ...,
                  stacklevel: int = ..., extra: Optional[Dict[str, Any]] = ...,
                  **kwargs: Any) -> None:
        self._handle_msg(msg, log_func="exception", **kwargs)

    def _handle_msg(self, msg, **kwargs):
        if (
            not self._logged_regularly_on_undefined_context(msg, **kwargs)
            and kwargs["context"] in self.active_contexts
        ):
            msg = f"[{kwargs.pop('context')}] {msg}"
            self._log_msg(msg, **kwargs)

    def _logged_regularly_on_undefined_context(self, msg: Any, **kwargs) -> bool:
        if "context" not in kwargs:
            self._log_msg(msg, **kwargs)
            return True
        return False

    def _log_msg(self, msg: Any, log_func: str, **kwargs):
        log_func = self.super_log_resolver[log_func]
        #self.real_logger.critical(f"to be logged with args: {kwargs}")
        log_func(str(msg), **kwargs)

    def setLevel(self, level: int) -> None:
        self.real_logger.setLevel(level)

    def getChild(self, suffix: str,
                 additional_contexts: Optional[Iterable[str]] = None) -> 'ContextLogger':
        child = self.real_logger.getChild(suffix)
        additional_contexts = set(additional_contexts) if additional_contexts else set()
        child = self._copy_with_new_child(child, additional_contexts)
        return child

    def _copy_with_new_child(self, child: Logger,
                             additional_contexts: Set[str]) -> 'ContextLogger':
        contexts = self.active_contexts | additional_contexts
        modified_copy = ContextLogger(self._host_name, self._logger_name, self._within_xterm,
                                      self._format, self.level, contexts, child)
        return modified_copy

    @property
    def propagate(self):
        return self.real_logger.propagate

    @propagate.setter
    def propagate(self, val):
        self.real_logger.propagate = val

    @property
    def level(self):
        return self.real_logger.level

    @level.setter
    def level(self, val):
        self.real_logger.level = val
