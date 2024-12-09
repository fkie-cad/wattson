import json
import shlex
import subprocess
from typing import TYPE_CHECKING

from wattson.networking.namespaces.nested_argument import NestedArgument

if TYPE_CHECKING:
    from wattson.networking.namespaces.docker_namespace import DockerNamespace


class DockerPopen(subprocess.Popen):
    pid_file_id: int = 0

    def __init__(self, cmd, **kwargs):
        # print(f"DockerPopen {cmd}")
        self._is_wrapped = kwargs.pop("docker_wrap", True)
        self._namespace: 'DockerNamespace' = kwargs.pop("namespace", None)
        if self._is_wrapped:
            self._id = DockerPopen.pid_file_id
            DockerPopen.pid_file_id += 1
            self._pid_file_name = f"/tmp/docker_pid_{self._id}.pid"
            if not isinstance(cmd, list):
                cmd = shlex.split(cmd)
            original_cmd = cmd[4:]
            full_cmd = ["echo $$", ">", f"{self._pid_file_name}", ";"] + original_cmd
            # full_cmd = shlex.quote(full_cmd)
            cmd = cmd[:4] + ["sh", "-c", str(NestedArgument(full_cmd))]
            # cmd = cmd[:4] + ["sh", "-c", f"{full_cmd}"]
            kwargs["shell"] = False
        # print(json.dumps(cmd, indent=4))
        super().__init__(cmd, **kwargs)

    def send_signal(self, sig):
        if self._is_wrapped:
            # Send signal to docker process
            code, lines = self._namespace.exec(["cat", self._pid_file_name], docker_wrap=False)
            shell_pid = lines[0]
            code, child_pids = self._namespace.exec(["pgrep", "--parent", shell_pid], docker_wrap=False)
            for pid in child_pids:
                self._namespace.logger.debug(f"Sending signal {sig} to PID {pid}")
                success, lines = self._namespace.exec(["kill", "-s", f"{sig}", pid], docker_wrap=False)
                if not success:
                    self._namespace.logger.error(f"Could not send signal (code {code})")
                    self._namespace.logger.error("\n".join(lines))

        # Send default signal
        super().send_signal(sig)
