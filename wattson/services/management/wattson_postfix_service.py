from typing import Optional, Callable, TYPE_CHECKING

from wattson.services.wattson_service import WattsonService
from wattson.util import get_logger

if TYPE_CHECKING:
    from wattson.services.configuration import ServiceConfiguration
    from wattson.cosimulation.simulators.network.components.wattson_network_node import WattsonNetworkNode
    from wattson.services.wattson_service_interface import WattsonServiceInterface


class WattsonPostfixService(WattsonService):
    def __init__(self, service_configuration: 'ServiceConfiguration', network_node: 'WattsonNetworkNode'):
        super().__init__(service_configuration, network_node)
        self.logger = get_logger("Mail Server", "Mail Server")

    def call(self, method, **kwargs):
        if method == "add_alias":
            user = kwargs.get("user")
            user_domain = kwargs.get("user_domain")
            alias = kwargs.get("alias")
            alias_domain = kwargs.get("alias_domain")
            code, _ = self.network_node.exec(["python3", "scripts/add-alias.py", user, user_domain, alias, alias_domain])
            return code == 0
        elif method == "remove_alias":
            user = kwargs.get("user")
            user_domain = kwargs.get("user_domain")
            alias = kwargs.get("alias")
            alias_domain = kwargs.get("alias_domain")
            code, lines = self.network_node.exec(
                ["python3", "scripts/delete-alias.py", user, user_domain, alias, alias_domain])
            return code == 0
        elif method == "add_user":
            user = kwargs.get("user")
            user_domain = kwargs.get("domain")
            password = kwargs.get("password_sha512")
            code, lines = self.network_node.exec(
                ["python3", "scripts/add-user.py", user, user_domain, password])
            return code == 0
        elif method == "remove_user":
            user = kwargs.get("user")
            user_domain = kwargs.get("domain")
            password = kwargs.get("password_sha512")
            code, lines = self.network_node.exec(
                ["python3", "scripts/delete-user.py", user, user_domain, password])
            return code == 0
        elif method == "modify_user":
            user = kwargs.get("user")
            user_domain = kwargs.get("domain")
            password = kwargs.get("password_sha512")
            target = kwargs.get("target")
            target_value = kwargs.get("target_value")
            code, lines = self.network_node.exec(
                ["python3", "scripts/update-user.py", user, user_domain, password, target, target_value])
            return code == 0
        elif method == "list_users":
            code, lines = self.network_node.exec(["python3", "scripts/list-users.py"])
            text = "\n".join(lines)
            return text
        return False

    def callable_methods(self) -> dict[str, dict]:
        return {
            "add_alias": {
                "parameters": {
                    "user": {"type": str, "description": "User to add an alias for"},
                    "user_domain": {"type": str, "description": "Domain of the user the alias is for."},
                    "alias": {"type": str, "description": "The alias to set for the user."},
                    "alias_domain": {"type": str, "description": "The domain of the alias."}
                },
                "returns": {"type": bool, "description": "Whether the operation was successful or not"},
                "description": "Adds a mail alias"
            },
            "remove_alias": {
                "parameters": {
                    "user": {"type": str, "description": "User to remove an alias for"},
                    "user_domain": {"type": str, "description": "Domain of the user the alias is for."},
                    "alias": {"type": str, "description": "The alias to remove for the user."},
                    "alias_domain": {"type": str, "description": "The domain of the alias."}
                },
                "returns": {"type": bool, "description": "Whether the operation was successful or not"},
                "description": "Removes a mail alias"
            },
            "add_user": {
                "parameters": {
                    "user": {"type": str, "description": "User to add."},
                    "domain": {"type": str, "description": "Domain for this user."},
                    "password_sha512": {"type": str, "description": "SHA-512 hash of the password."}
                },
                "returns": {"type": bool, "description": "Whether the operation was successful or not"},
                "description": "Adds a mail user"
            },
            "remove_user": {
                "parameters": {
                    "user": {"type": str, "description": "User to remove."},
                    "domain": {"type": str, "description": "Domain for this user."},
                    "password_sha512": {"type": str, "description": "SHA-512 hash of the password."}
                },
                "returns": {"type": bool, "description": "Whether the operation was successful or not"},
                "description": "Removes a mail user"
            },
            "modify_user": {
                "parameters": {
                    "user": {"type": str, "description": "User to add."},
                    "domain": {"type": str, "description": "Domain for this user."},
                    "password_sha512": {"type": str, "description": "SHA-512 hash of the password."},
                    "target": {"type": str, "description": "The field to be modified. (user, domain, password)"},
                    "target_value": {"type": str, "description": "The new value for the target field."}
                },
                "returns": {"type": bool, "description": "Whether the operation was successful or not"},
                "description": "Modifies a mail user"
            },
            "list_users": {
                "parameters": {},
                "returns": {"type": str, "description": "A list of users with their aliases."},
                "description": "Lists mail users with aliases"
            }
        }

    def get_start_command(self) -> list[str]:
        # self.write_configuration()
        return [self.network_node.get_python_executable(), "/wattson/scripts/start.py", str(self.get_current_guest_configuration_file_path().absolute())]

    def stop(self, wait_seconds: float = 5, auto_kill: bool = False,
             async_callback: Optional[Callable[['WattsonServiceInterface'], None]] = None) -> bool:
        success = super().stop(wait_seconds=wait_seconds, auto_kill=auto_kill, async_callback=async_callback)
        self.network_node.exec(["postfix", "stop"])
        return success

    def get_stderr(self):
        return self.get_stdout()

    def get_stdout(self):
        return self.get_log_handle()
