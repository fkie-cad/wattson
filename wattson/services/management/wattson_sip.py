from typing import Optional, Callable, List

from wattson.services.wattson_service import WattsonService
from wattson.util import get_logger


class WattsonSIPService(WattsonService):
    def __init__(self, service_configuration: 'ServiceConfiguration', network_node: 'WattsonNetworkNode'):
        super().__init__(service_configuration, network_node)
        self.logger = get_logger("SIP Server", "SIP Server")

    def callable_methods(self) -> dict[str, dict]:
        return {
            "add_user": {
                "parameters": {
                    "name": {"type": str, "description": "Name of the user"},
                    "domain": {"type": str, "description": "Domain of the user"},
                    "password": {"type": str, "description": "Password of the user"}
                },
                "returns": {
                    "type": bool, "description": "Whether the operation was successful or not."
                },
                "description": "Add a user."
            },
            "add_alias": {
                "parameters": {
                    "alias": {"type": str, "description": "The alias to add"},
                    "alias_domain": {"type": str, "description": "Domain of the alias."},
                    "user": {"type": str, "description": "Name of the user."},
                    "user_domain": {"type": str, "description": "Domain of the user."}
                },
                "returns": {
                    "type": bool, "description": "Whether the operation was successful or not."
                },
                "description": "Add an alias for a user."
            },
            "remove_user": {
                "parameters": {
                    "name": {"type": str, "description": "Name of the user"},
                    "domain": {"type": str, "description": "Domain of the user"},
                    "password": {"type": str, "description": "Password of the user"}
                },
                "returns": {
                    "type": bool, "description": "Whether the operation was successful or not."
                },
                "description": "Remove the specified user."
            },
            "remove_alias": {
                "parameters": {
                    "alias": {"type": str, "description": "The alias to remove."},
                    "alias_domain": {"type": str, "description": "Domain of the alias."},
                    "user": {"type": str, "description": "Name of the user."},
                    "user_domain": {"type": str, "description": "Domain of the user."}
                },
                "returns": {
                    "type": bool, "description": "Whether the operation was successful or not."
                },
                "description": "Remove the specified alias."
            },
            "modify_user": {
                "parameters": {
                    "name": {"type": str, "description": "Name of the user"},
                    "domain": {"type": str, "description": "Domain of the user"},
                    "password": {"type": str, "description": "Password of the user"},
                    "target": {"type": str, "description": "The field to be changed."},
                    "target_value": {"type": str, "description": "The new value for the field to be changed. (name, domain, password)"}
                },
                "returns": {
                    "type": bool, "description": "Whether the operation was successful or not."
                },
                "description": "Modify the specified user and set target to target value."
            },
            "list_users": {
                "parameters": {},
                "returns": {"type": str, "description": "A list of users with their aliases."},
                "description": "Returns a list of all users with their aliases."
            }
        }

    def call(self, method, **kwargs):
        if method == "add_user":
            code, _ = self.network_node.exec(["python3", "scripts/add-user.py", kwargs.get("name"), kwargs.get("domain"), kwargs.get("domain")])
            return code == 0
        elif method == "remove_user":
            code, _ = self.network_node.exec(["python3", "scripts/delete-user.py", kwargs.get("name"), kwargs.get("domain"), kwargs.get("domain")])
            return code == 0
        elif method == "add_alias":
            code, _ = self.network_node.exec(["python3", "scripts/add-alias.py", kwargs.get("alias"), kwargs.get("alias_domain"), kwargs.get("user"), kwargs.get("user_domain")])
            return code == 0
        elif method == "remove_alias":
            code, _ = self.network_node.exec(["python3", "scripts/delete-alias.py", kwargs.get("alias"), kwargs.get("alias_domain"), kwargs.get("user"), kwargs.get("user_domain")])
            return code == 0
        elif method == "modify_user":
            code, _ = self.network_node.exec(
                ["python3", "scripts/update-user.py", kwargs.get("name"), kwargs.get("domain"), kwargs.get("password"),
                 kwargs.get("target"), kwargs.get("target_value")])
            return code == 0
        elif method == "list_users":
            code, lines = self.network_node.exec(["python3", "scripts/list-users.py"])
            return lines
        return False

    def get_start_command(self) -> List[str]:
        # self.write_configuration()
        return [self.network_node.get_python_executable(), "/wattson/scripts/start.py", str(self.get_current_guest_configuration_file_path().absolute())]

    def stop(self, wait_seconds: float = 5, auto_kill: bool = False, async_callback: Optional[Callable[['WattsonServiceInterface'], None]] = None) -> bool:
        success = super().stop(wait_seconds, auto_kill, async_callback=async_callback)
        return success
