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


        Returns:
            List[str]: The start command as a list of strings.
        """
        ...

    @abc.abstractmethod
    def get_priority(self) -> ServicePriority:
        """
        Returns the service's priority


        Returns:
            ServicePriority: The associated ServicePriority object
        """
        ...

    @abc.abstractmethod
    def start(self, refresh_config: bool = False) -> bool:
        """
        Start the service.

        Args:
            refresh_config (bool, optional):
                Whether to refresh the config even if it already exists.
                (Default value = False)

        Returns:
            bool: True iff the service has been started.
        """
        ...

    @abc.abstractmethod
    def stop(self, wait_seconds: float = 5, auto_kill: bool = False, async_callback: Optional[Callable[['WattsonServiceInterface'], None]] = None) -> bool:
        """
        Stop the service.

        Args:
            wait_seconds (float, optional):
                Number of seconds to wait for the service to gracefully terminate.
                (Default value = 5)
            auto_kill (bool, optional):
                Whether to kill the service automatically after the waiting timeout has been exceeded.
                (Default value = False)
            async_callback (Optional[Callable[['WattsonServiceInterface'], None]], optional):
                An optional callback to call once the service has terminated. Makes the stop method return immediately.
                (Default value = None)

        Returns:
            bool: None if an async_callback is given, else True iff the service has been terminated.
        """
        ...

    @abc.abstractmethod
    def restart(self, refresh_config: bool = False) -> bool:
        """
        Restarts the service. Shortcut for (blocking) stop and start calls.

        Args:
            refresh_config (bool, optional):
                Whether to refresh the config even if it already exists
                (Default value = False)

        Returns:
            bool: Whether the service has been restarted successfully.
        """
        ...

    @abc.abstractmethod
    def kill(self) -> bool:
        """
        Sends the SIGKILL to the process.


        Returns:
            bool: True iff the service has been terminated.
        """
        ...

    @abc.abstractmethod
    def is_running(self) -> bool:
        """
        


        Returns:
            bool: Whether the service is currently running.
        """
        ...

    @abc.abstractmethod
    def is_killed(self) -> bool:
        """
        


        Returns:
            bool: Whether the service has been killed.
        """
        ...

    @abc.abstractmethod
    def get_pid(self) -> Optional[int]:
        """
        


        Returns:
            Optional[int]: The PID of the service process or None if the service is not running.
        """
        ...

    @abc.abstractmethod
    def poll(self) -> Optional[int]:
        """
        Polls the service process.


        Returns:
            Optional[int]: None or the return code of the process.
        """
        ...

    @abc.abstractmethod
    def wait(self, timeout: Optional[float] = None) -> int:
        """
        Waits for the service process to terminate.
        If a timeout is given and the process does not terminate during the timeout, a TimeoutExpired exception is thrown.

        Args:
            timeout (Optional[float], optional):
                An optional timeout.
                (Default value = None)

        Returns:
            int: The processes return code.
        """
        ...
