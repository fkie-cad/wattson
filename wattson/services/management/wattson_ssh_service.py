import shlex
import time

from wattson.services.wattson_service import WattsonService

from typing import Optional, Callable, TYPE_CHECKING, List

from wattson.util.performance.performance_decorator import performance_assert

if TYPE_CHECKING:
    from wattson.services.configuration import ServiceConfiguration
    from wattson.cosimulation.simulators.network.components.wattson_network_node import WattsonNetworkNode
    from wattson.services.wattson_service_interface import WattsonServiceInterface


class WattsonSshService(WattsonService):
    def __init__(self, service_configuration: 'ServiceConfiguration', network_node: 'WattsonNetworkNode'):
        super().__init__(service_configuration, network_node)
        self.config = service_configuration
        self._last_status_call = 0
        self._last_status = None
        self._status_timeout = 30

    def callable_methods(self) -> dict[str, dict]:
        return {
            "change_password": {
                "parameters": {
                    "user": {"type": str, "description": "Name of the user."},
                    "password": {"type": str, "description": "The new password for the user."}
                },
                "returns": {"type": bool, "description": "Whether the operation was successful or not."},
                "description": "Change password of a user."
            },
            "enable_user": {
                "parameters": {
                    "user": {"type": str, "description": "The user to enable."}
                },
                "returns": {
                    "type": bool, "description": "Whether the operation was successful or not."
                },
                "description": "Enable the specified user."
            },
            "disable_user": {
                "parameters": {
                    "user": {"type": str, "description": "The user to disable."}
                },
                "returns": {
                    "type": bool, "description": "Whether the operation was successful or not."
                },
                "description": "Disbale the specified user."
            }
        }

    def call(self, method, **kwargs):
        if method == "change_password":
            code, _ = self.network_node.exec(["echo", f"'{kwargs.get('password')}\n{kwargs.get('password')}'", "|", "passwd", kwargs.get("user")])
            return code == 0
        elif method == "enable_user":
            code, _ = self.network_node.exec(["passwd", "-u", kwargs.get("user")])
            return code == 0
        elif method == "disable_user":
            code, _ = self.network_node.exec(["passwd", "-l", kwargs.get("user")])
            return code == 0
        return False

    def start(self, refresh_config: bool = False):
        self.network_node.exec(["service", "ssh", "stop"])

        user_config = self.config.get("users", {})
        for user, password in user_config.items():
            self._create_user_if_not_existing(user)
            self._set_user_password(user, password)
            self._enable_ssh_user(user)

        code, _ = self.network_node.exec(["service", "ssh", "start"])
        self._last_status = code == 0
        self._last_status_call = time.time()

    @performance_assert(1)
    def update_is_running(self):
        if not self.network_node.is_started:
            self._last_status = False
            return
        self.network_node.logger.info(f"Updating SSH service status {self.name} // {self.id}")
        code, _ = self.network_node.exec(["service", "ssh", "status"])
        self._last_status = code == 0
        self._last_status_call = time.time()

    def update(self):
        self.update_is_running()

    @performance_assert(1)
    def is_running(self) -> bool:
        if self._last_status is not None:
            return self._last_status
        self.update_is_running()
        return self._last_status

    def kill(self) -> bool:
        return self.stop()

    def get_start_command(self) -> List[str]:
        return []
        # return ["/usr/sbin/sshd"]

    def stop(self, wait_seconds: float = 5, auto_kill: bool = False, async_callback: Optional[Callable[['WattsonServiceInterface'], None]] = None) -> bool:
        code, _ = self.network_node.exec(["service", "ssh", "stop"])
        self._last_status = not (code == 0)
        self._last_status_call = time.time()
        return not self.is_running()

    def _set_user_password(self, user, password):
        # self.network_node.logger.info(f"Updating password for {user}")
        if self.network_node.exec(["echo", shlex.quote(f"{user}:{password}"), "|", "chpasswd"], shell=True)[0] != 0:
            self.network_node.logger.error(f"Could not set password for {user}")
            return False
        return True

    def _enable_ssh_user(self, user):
        pass

    def _user_exists(self, user) -> bool:
        code, out = self.network_node.exec(["id", user])
        return code == 0

    def _create_user_if_not_existing(self, user):
        if self._user_exists(user):
            return
        # User does not exists
        # self.network_node.logger.info(f"Creating user {user}")
        code, out = self.network_node.exec(["useradd", "-rm", "-d", f"/home/{user}", "-s", "/bin/bash", "-g", "root", "-G", "sudo", user])
        # code, out = self.network_node.exec(["adduser", user, "-s", "/bin/bash"])
        if code != 0:
            self.network_node.logger.error(f"Could not create user {user}")
            self.network_node.logger.error(f"\n".join(out))
