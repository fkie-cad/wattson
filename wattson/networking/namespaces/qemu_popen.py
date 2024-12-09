import base64
import json
import shlex
import signal
import subprocess
import time
import traceback
from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    from wattson.networking.namespaces.virtual_machine_namespace import VirtualMachineNamespace


class QemuPopen(subprocess.Popen):
    def __init__(self, cmd, **kwargs):
        self._namespace: 'VirtualMachineNamespace' = kwargs.pop('namespace')
        self._qemu_pid = None
        self._return_code = None
        self._return_data = None
        self._return_error = None
        self._logger = self._namespace.logger.getChild('QemuPopen')

        if isinstance(cmd, str):
            cmd = shlex.split(cmd)

        self._cmd = cmd

        # Split arguments
        native_command = self._wrap_command(cmd)
        self._full_command = native_command
        try:
            output = subprocess.check_output(native_command, stderr=subprocess.STDOUT)
            result = json.loads(output)
            self._qemu_pid = result["return"]["pid"]
        except subprocess.CalledProcessError as e:
            self._logger.error(f"Could not start qemu process for {' '.join(cmd)}")
            raise e
        except Exception as e:
            self._logger.error(f"Error while starting qemu process")
            self._logger.error(f"Could not start qemu process for {' '.join(cmd)}")
            self._logger.error(traceback.format_exc())
            raise e
        # There is no super class call by design!

    def _wrap_command(self, cmd: List, execute: str = "guest-exec"):
        path = cmd[0]
        arguments = cmd[1:]
        qemu_config = {
            "execute": execute,
            "arguments": {
                "path": path,
                "arg": arguments,
                "capture-output": True
            }
        }
        return ["virsh", "-c", "qemu:///system", "qemu-agent-command", self._namespace.domain_name, json.dumps(qemu_config)]

    def poll(self):
        if self._qemu_pid is None:
            self._logger.warning(f"No PID given")
            return None

        if self._return_code is not None:
            self._logger.warning(f"Already terminated")
            return self._return_code

        try:
            qemu_config = {
                "execute": "guest-exec-status",
                "arguments": {
                    "pid": self._qemu_pid
                }
            }
            output = subprocess.check_output(["virsh", "-c", "qemu:///system", "qemu-agent-command", self._namespace.domain_name, json.dumps(qemu_config)])
            result = json.loads(output)
            if "return" in result:
                if result["return"].get("exited"):
                    self._return_data = base64.b64decode(result["return"].get("out-data", "")).decode("utf-8")
                    self._return_error = base64.b64decode(result["return"].get("err-data", "")).decode("utf-8")
                    self._return_code = result["return"].get("exitcode", -1)
                    """
                    if self._return_code != 0:
                        self._logger.error(f"QEMU Guest Process had error: {self._return_code}")
                        self._logger.error(" ".join(self._cmd))
                        self._logger.error(" ".join(self._full_command))
                        self._logger.error(self._return_data)
                        self._logger.error(self._return_error)
                    """
                    return result["return"].get("exitcode", -1)
                return None
            self._logger.error(f"Poll result error")
            self._logger.error(repr(result))
            return None
        except subprocess.CalledProcessError as e:
            self._logger.error(f"Could not poll process")
            self._logger.error(traceback.format_exc())
            return None
        except Exception as e:
            self._logger.error(f"Failed to poll process")
            self._logger.error(traceback.format_exc())
            return None

    def wait(self, timeout=None, _interval: float = 0.05):
        start_time = time.perf_counter()
        while self.poll() is None:
            if timeout is not None:
                wait_time = time.perf_counter() - start_time
                if wait_time > timeout:
                    raise subprocess.TimeoutExpired(f"Timed out waiting for process {self._qemu_pid}")
            time.sleep(_interval)
        return self._return_code

    def communicate(self, input=None, timeout=None):
        if input is not None:
            raise NotImplementedError("QemuPopen.communicate() not implemented with input")
        self.wait(timeout)
        return self._return_data, self._return_error

    def send_signal(self, sig):
        try:
            subprocess.check_call(self._wrap_command(["kill", "-s", str(sig)]))
        except subprocess.CalledProcessError as e:
            self._logger.error(f"Could not send signal")
            self._logger.error(traceback.format_exc())
        finally:
            pass

    def terminate(self):
        self.send_signal(signal.SIGTERM)

    def kill(self):
        self.send_signal(signal.SIGKILL)

    @property
    def pid(self):
        return self._qemu_pid

    @property
    def returncode(self):
        return self._return_code
