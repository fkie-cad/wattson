import abc
from typing import Optional, Callable, List, Union

from wattson.services.service_priority import ServicePriority


class WattsonServiceInterface(abc.ABC):
    id: int
    name: str

    @abc.abstractmethod
    def call(self, method, **kwargs) -> Union[bool, str]:
        ...

    @abc.abstractmethod
    def callable_methods(self) -> dict[str, dict]:
        ...

    @abc.abstractmethod
    def get_start_command(self) -> List[str]:
        """
        Returns the command to be executed for starting this service on the network node
        @return: The start command as a list of strings.
        """
        ...

    @abc.abstractmethod
    def get_priority(self) -> ServicePriority:
        """
        Returns the service's priority
        @return: The associated ServicePriority object
        """
        ...

    @abc.abstractmethod
    def start(self, refresh_config: bool = False) -> bool:
        """
        Start the service.
        @param refresh_config: Whether to refresh the config even if it already exists.
        @return: True iff the service has been started.
        """
        ...

    @abc.abstractmethod
    def stop(self, wait_seconds: float = 5, auto_kill: bool = False, async_callback: Optional[Callable[['WattsonServiceInterface'], None]] = None) -> bool:
        """
        Stop the service.
        @param wait_seconds: Number of seconds to wait for the service to gracefully terminate.
        @param auto_kill: Whether to kill the service automatically after the waiting timeout has been exceeded.
        @param async_callback: An optional callback to call once the service has terminated. Makes the stop method return immediately.
        @return: None if an async_callback is given, else True iff the service has been terminated.
        """
        ...

    @abc.abstractmethod
    def restart(self, refresh_config: bool = False) -> bool:
        """
        Restarts the service. Shortcut for (blocking) stop and start calls.
        @param refresh_config: Whether to refresh the config even if it already exists
        @return: Whether the service has been restarted successfully.
        """
        ...

    @abc.abstractmethod
    def kill(self) -> bool:
        """
        Sends the SIGKILL to the process.
        @return: True iff the service has been terminated.
        """
        ...

    @abc.abstractmethod
    def is_running(self) -> bool:
        """
        @return: Whether the service is currently running.
        """
        ...

    @abc.abstractmethod
    def is_killed(self) -> bool:
        """
        @return: Whether the service has been killed.
        """
        ...

    @abc.abstractmethod
    def get_pid(self) -> Optional[int]:
        """
        @return: The PID of the service process or None if the service is not running.
        """
        ...

    @abc.abstractmethod
    def poll(self) -> Optional[int]:
        """
        Polls the service process.
        @return: None or the return code of the process.
        """
        ...

    @abc.abstractmethod
    def wait(self, timeout: Optional[float] = None) -> int:
        """
        Waits for the service process to terminate.
        If a timeout is given and the process does not terminate during the timeout, a TimeoutExpired exception is thrown.
        @param timeout: An optional timeout.
        @return: The processes return code.
        """
        ...
