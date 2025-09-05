from typing import Optional, List, TYPE_CHECKING

from wattson.powergrid.remote.remote_power_grid_model import RemotePowerGridModel

if TYPE_CHECKING:
    from wattson.cosimulation.cli.cli import CLI

from wattson.cosimulation.cli.cli_command_handler import CliCommandHandler


class GridValueCliCommand(CliCommandHandler):
    def __init__(self, cli: 'CLI'):
        super().__init__(cli)
        self.cli.register_command("gridvalue", self)
        self.remote_power_grid_model = RemotePowerGridModel.get_instance(wattson_client=self.cli.wattson_client)
        self._available_commands = [
            "set", "get", "lock", "unlock", "freeze", "unfreeze", "forceset"
        ]

    def strtobool(self, val):
        """
        Convert a string representation of truth to true (1) or false (0).
        True values are 'y', 'yes', 't', 'true', 'on', and '1'; false values are 'n', 'no', 'f', 'false', 'off', and '0'.  Raises ValueError
        if 'val' is anything else.

        Args:
            val:
                
        """
        val = val.lower().strip()
        if val in ["y", "yes", "t", "true", "on", "1"]:
            return True
        elif val in ["n", "no", "f", "false", "off", "0"]:
            return False
        else:
            raise ValueError("invalid truth value %r" % (val,))

    def parse_value(self, grid_value, value):
        try:
            return self.strtobool(value)
        except ValueError as e:
            try:
                return float(value)
            except:
                print(f"{value} / {type(value)}")
                print(f"{e=}")
                return value

    def handle_command(self, command: List[str], prefix: List[str]) -> bool:
        if command[0] not in self._available_commands or len(command) < 2:
            print("Invalid command")
            return True
        if command[0] in ["set", "forceset", "freeze"] and len(command) != 3:
            print("Invalid command")

        grid_value_identifier = command[1]
        grid_value = self.remote_power_grid_model.get_grid_value_by_identifier(grid_value_identifier)
        if command[0] == "get":
            print(f"Is Locked: {grid_value.is_locked}")
            print(f"Is Frozen: {grid_value.is_frozen}")
            if grid_value.is_frozen:
                print(f"Frozen Value: {grid_value.get_frozen_value()}")
                print(f"Internal Value: {grid_value.raw_get_value(override_freeze=True)}")
            print(f"{grid_value.get_identifier()} = {grid_value.get_value()} ({grid_value.scale.get_prefix()}{grid_value.unit.get_symbol()})")
            return True
        if command[0] == "unfreeze":
            print(f"Unfreezing {grid_value.get_identifier()}")
            grid_value.unfreeze()
            return True
        if command[0] == "lock":
            print(f"Locking {grid_value.get_identifier()}")
            grid_value.lock()
            return True
        if command[0] == "unlock":
            print(f"Unlocking {grid_value.get_identifier()}")
            grid_value.unlock()
            return True

        if command[0] == "set":
            value = self.parse_value(grid_value, command[2])
            print(f"Setting {grid_value.get_identifier()} = {value} {type(value)}")
            grid_value.set_value(value)
            if grid_value.is_locked:
                print(f"  GridValue is locked for setting. Use 'forceset' instead")
            return True
        if command[0] == "forceset":
            value = self.parse_value(grid_value, command[2])
            print(f"Forcing setting {grid_value.get_identifier()} = {value}")
            grid_value.set_value(value, override_lock=True)
            return True
        if command[0] == "freeze":
            value = self.parse_value(grid_value, command[2])
            print(f"Freezing {grid_value.get_identifier()} as {value}")
            grid_value.freeze(value)
            return True

    def auto_complete_choices(self, prefix: List[str], level: Optional[int] = None) -> dict:
        grid_values = self.remote_power_grid_model.get_grid_values()
        completion_dict = {"gridvalue": {
            "children": {
                "get": {"children": {
                    grid_value.get_identifier(): {
                        "children": None,
                        "description": None
                    }
                    for grid_value in grid_values
                }, "description": "Get power grid value"},
                "set": {"children": {
                    grid_value.get_identifier(): {
                        "children": None,
                        "description": None
                    }
                    for grid_value in grid_values
                }, "description": "Set power grid value"}
            },
            "description": "Interact with GridValues"
        }}
        return completion_dict

    def help(self, prefix: List[str], subcommand: Optional[List[str]] = None) -> Optional[str]:
        return "Set or get GridValues"

    def description(self, prefix: List[str]) -> str:
        return "Set or get GridValues"
