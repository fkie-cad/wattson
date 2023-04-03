import threading as th
from typing import Union, Optional
from pathlib import Path
from time import sleep
import logging
import queue
import zmq
import os
import sys

from wattson.apps.interface.clients.publisher_client import SubscriptionClient
from wattson.apps.interface.util.constants import *


class DumpSubscriber(th.Thread):
    def __init__(
        self,
        mtu_ip: str = DEFAULT_PUB_SERVER_IP,
        mtu_port: int = DEFAULT_PUB_SERVER_PORT,
        dump_path: Optional[Union[str, Path]] = None,
        format: str = "CSV",
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__()
        self.logger = logger
        self.mtu_ip = mtu_ip
        self.mtu_port = mtu_port
        self.out_path = Path(dump_path) if dump_path is not None else dump_path
        self.out_format = format
        self.name = "Dump App"
        self.client = SubscriptionClient(mtu_ip, mtu_port, self.logger, self.name)
        self._stopped = th.Event()
        self._poll_t = 2.0
        if self.out_path:
            if self.out_path.exists():
                print(
                    f"starting subscriber {self.name} will overwrite "
                    f"dump output file {self.out_path}",
                    file=sys.err,
                )
        elif self.out_path:
            dirname = self.out_path.parent
            os.makedirs(dirname)
        else:
            self.out_path = Path("/tmp") / f"test_mtu_{mtu_ip}_{mtu_port}_dump.log"

        self.f = None
        self.reference_cnt = 0
        self.prefix = ""
        self.subscriber_type = "Historian"  # actually only necessary for ones that send cmds

    def run(self):
        if self.out_path is None:
            raise RuntimeError("Cannot find dump location")

        if self.out_path.exists():
            print(f"overwriting dump output file {self.out_path}")
        self.f = open(self.out_path, "w")

        self.connect()

        while not self._stopped.is_set() and self.client.connected.is_set():
            try:
                msg = self.client.read_messages.get(timeout=self._poll_t)
            except queue.Empty:
                continue
            self.f.write(str(msg) + "\n")

    def connect(self, wait_t: float = 3.0):
        while self._stopped.is_set() and not self.client.connected.is_set():
            if not self.client.started.is_set():
                try:
                    self.client.start()
                    return
                except zmq.ZMQError as e:
                    self.logger.critical(f"got ZMQ error {e}")
                    raise e
            sleep(wait_t)

    def stop(self):
        self.client.stop()
        self.f.close()
        self._stopped.set()
