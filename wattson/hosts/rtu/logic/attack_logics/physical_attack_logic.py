import os
import signal
import subprocess
import threading
import time
from typing import Optional

from wattson.hosts.rtu.rtu_logic import RTULogic


class PhysicalAttackLogic(RTULogic):
    """RTU Logic that simulates a crash after a waiting period"""

    def __init__(self, rtu: 'RTU', **kwargs):
        super().__init__(rtu, **kwargs)
        self._delay = kwargs.get("delay", 30)
        self._disconnect_switches = kwargs.get("disconnect_switches", ["834.10240"])
        self._interface = kwargs.get("interface", "n834-eth0")
        self._terminate = threading.Event()
        self._monitored_dps = {}
        self._thread = None

    def on_start(self):
        super().on_start()
        self._thread = threading.Thread(target=self.simulate_crash)
        self._thread.start()

    def simulate_crash(self):
        self.logger.info(f"Waiting for Wattson Client...")
        self.rtu.wattson_client.require_connection()
        self.logger.info(f"Waiting for MTU READY...")
        self.rtu.wattson_client.event_wait(event_name="MTU_READY", timeout=1)
        self.logger.info(f"Waiting for {self._delay} seconds before crash...")
        target_time = time.time() + self._delay
        while time.time() < target_time:
            time.sleep(1)
            if self._terminate.is_set():
                self.logger.info(f"Terminating...")
                return
        self.logger.info(f"Bring down interface...")
        cmd = " ".join(['ip link set dev', self._interface, 'down'])
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, error = p.communicate()
        if p.returncode != 0:
            self.logger.error(error)
        else:
            self.logger.info(output)
        self.logger.info(f"Stopping measurement updates...")
        self.rtu.manager.block_reads()
        self.logger.info(f"Disabling Grid components...")
        for dp_id in self._disconnect_switches:
            self.logger.info(f"  Opening Switch {dp_id}...")
            self.rtu.manager.set_value(dp_id, False)
        self.logger.info(f"Kill RTU...")
        os.kill(os.getpid(), signal.SIGKILL)

    def on_stop(self):
        super().on_stop()
        self._terminate.set()

    def join(self, timeout: Optional[float] = None):
        self._thread.join(timeout)

    def configure(self):
        super().configure()
