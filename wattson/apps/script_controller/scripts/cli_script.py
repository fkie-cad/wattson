import time
from threading import Event
import re
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from wattson.apps.script_controller import ScriptControllerApp

from wattson.apps.script_controller.interface import SoloScript
from wattson.deployment.cli.messages import CommandMessage, SystemMessage, SystemMessageType
from wattson.deployment.cli.server.server import CLIServer


class CLIScript(SoloScript):
    def __init__(self, controller: 'ScriptControllerApp'):
        super().__init__(controller)
        self.logger = None
        self.shutdown = Event()
        self.cli_server = CLIServer(ip=controller.host_ip,
                                    cmd_callback=self.cmd_callback,
                                    system_callback=self.system_callback)

    def run(self):
        self.logger = self.controller.logger.getChild("CLIScript")
        self.cli_server.start()
        self.logger.info("CLI Ready...")
        while not self.shutdown.is_set():
            self.cli_server.wait_for_connect()
            self.logger.info("CLI Connected...")
            self.cli_server.wait_for_disconnect()
            self.logger.info("CLI Disconnected...")
        self.cli_server.stop()

    def cmd_callback(self, message: CommandMessage):
        self.logger.info(message.string_cmd)
        p = message.parse()
        self.logger.info(p)
        if p["cmd"][0] == "":
            self.cli_server.send_ok()
        elif p["cmd"][0] == "exit":
            self.cli_server.send(SystemMessage.factory(SystemMessageType.DISCONNECT))
        elif p["cmd"][0] == "shutdown":
            self.cli_server.print("Requesting Simulation Shutdown...")
            ok = self.controller.coord_client.request_shutdown()
            self.cli_server.print(f"...{'OK' if ok else 'Failed'}")
            self.cli_server.send(SystemMessage.factory(SystemMessageType.DISCONNECT))
        elif p["cmd"][0] == "sleep":
            if len(p["cmd"]) > 1:
                try:
                    t = float(p["cmd"][1])
                    self.cli_server.print(f"Sleeping {t} seconds")
                    time.sleep(t)
                    self.cli_server.send_ok()
                    return False
                except ValueError:
                    self.cli_server.print("Invalid sleep interval")
            self.cli_server.send_unblock()
        elif p["cmd"][0] == "echo":
            if len(p["cmd"]) > 1:
                self.cli_server.print(" ".join(p["cmd"][1:]))
            else:
                self.cli_server.print("")
            self.cli_server.send_ok()
        elif not self._handle_command(message):
            self.cli_server.print("Unknown command")
            self.cli_server.send_unblock()
        return False

    def system_callback(self, message: SystemMessage):
        self.logger.info(f"[SYS] {message.sys_message_type.name}")
        return False

    def _handle_command(self, message: CommandMessage) -> bool:
        p = message.parse()
        main = p["cmd"][0]
        cmd = p["cmd"]
        if main == "set":
            if len(cmd) == 4:
                coa: str = cmd[1]
                ioa: str = cmd[2]
                value = cmd[3]
                value = self._parse_value(value)
                if self.validate_set_cmd(coa, ioa, value):
                    self.controller.set_dp(int(coa), int(ioa), value)
                    self.cli_server.send_unblock()
                else:
                    self.cli_server.print("Invalid COA, IOA or VALUE")
                    self.cli_server.send_unblock()
            else:
                self.cli_server.print("Expected 'set $COA $IOA $VALUE'")
                self.cli_server.send_unblock()
            return True
        elif main == "get":
            if len(cmd) == 3:
                coa: str = cmd[1]
                ioa: str = cmd[2]
                if coa.isdigit() and ioa.isdigit():
                    val = self.controller.get_dp(int(coa), int(ioa))
                    self.cli_server.print(f"{coa}.{ioa}  -  {val}")
                    self.cli_server.send_unblock()
            else:
                self.cli_server.print("Expected 'get $COA $IOA'")
                self.cli_server.send_unblock()
            return True

        return False

    def _parse_value(self, value: str):
        if value.lower() == "true":
            return True
        elif value.lower() == "false":
            return False
        elif value.isdigit():
            return int(value)
        elif re.match('^[0-9]*\.[0-9]*$', value):
            if value == ".":
                value = "0"
            return float(value)
        return None

    def validate_set_cmd(self, coa, ioa, value):
        return coa.isdigit() and ioa.isdigit() and value is not None

