import logging
import random
import threading
import time
from typing import Optional

from wattson.apps.interface.util import messages as msgs
from wattson.apps.interface.clients import CommandClient
from wattson.apps.interface.util.constants import DEFAULT_PUB_SERVER_IP, DEFAULT_CMD_SERVER_PORT
from wattson.iec104.interface.types import TypeID


class SwapSwitchSubscriber(threading.Thread):
    """ Sort of Deprecated, used for original testing

    """
    def __init__(
        self,
        mtu_ip: str = DEFAULT_PUB_SERVER_IP,
        mtu_port: int = DEFAULT_CMD_SERVER_PORT,
        logger: Optional[logging.Logger] = None,
        mtu=None,
        ups: float = 0.1,
    ):
        super().__init__()
        self.logger = logger.getChild("swap_Switch_Controller")
        self.logger.setLevel(logging.DEBUG)
        self.mtu_ip = mtu_ip
        self.mtu_port = mtu_port
        self.name = "Swap Switch App"
        self.client = CommandClient(mtu_ip, mtu_port, self.logger, self.name)
        self._stopped = threading.Event()
        self.wait_t = 1 / ups
        self.subscriber_type = "SwitchSwapper"
        self.reference_prefix = ""
        self.reference_cnt = 0

    def run(self):
        self.logger.debug("Starting Automatic Switches Subscriber")
        init_msg = msgs.SubscriptionInitMsg(self.subscriber_type).to_json()
        self.client.command_messages.put(init_msg)
        self.client.start()
        init_reply = self.client.read_messages.get(block=True, timeout=180)
        self.reference_prefix = init_reply.subscriber_ID
        self.logger.info(f"Received prefix {self.reference_prefix}")

        last_update = time.time()
        while not self._stopped.is_set():
            if time.time() > last_update + self.wait_t:
                self.client.command_messages.put(self.random_cmd().to_json())
                last_update = time.time()

    @property
    def next_reference_cnt(self):
        self.reference_cnt += 1
        return self.reference_cnt - 1

    def stop(self):
        self.client.stop()
        self._stopped.set()

    def random_cmd(self, coa=163):
        cmd = random.randint(0, 10)
        if cmd < 8:
            return self.switch_command(coa)
        elif cmd == 9:
            return msgs.TotalInterroReq()
        else:
            return msgs.RTUStatusReq()

    def switch_command(self, coa=163):
        bad_coas = [0, 12, 1300, 5000]
        bad_types = [TypeID.P_AC_NA_1, TypeID.M_SP_NA_1, TypeID.C_DC_NA_1]
        correct_ioa = True  # random.choice((True, False))
        read = random.choice((True, False))
        if correct_ioa:
            self.logger.debug("sending valid switch change")
        else:
            self.logger.warning("Sending cmd with bad IOA")
        ioa = self.command_ioa(correct_ioa, read)
        invalid_coa = False  # random.choice((True, False))
        bad_type = False
        if invalid_coa:
            coa = random.choice(bad_coas)
            self.logger.warning("Sending cmd with bad COA")
        if bad_type:
            type_ID = random.choice(bad_types)
        else:
            type_ID = TypeID.C_SC_NA_1
        val = random.choice((True, False))

        if read:
            msg = msgs.ReadDatapoint(
                coa, ioa, f"{self.reference_prefix}_{self.next_reference_cnt}"
            )
        else:
            msg = msgs.ProcessInfoControl(
                coa,
                type_ID,
                {ioa: val},
                reference_nr=f"{self.reference_prefix}_{self.next_reference_cnt}",
            )
        return msg

    @staticmethod
    def command_ioa(valid_ioa: bool, read: bool):
        bad_ioas = [0, 1000, 2345, 2345981, 1202002023]
        actuator_switch_ioas = [35110, 31110, 32110, 33110, 34110]
        sensor_switch_ioas = [25110, 21110, 22110, 23110, 24110]
        if not valid_ioa:
            return random.choice(bad_ioas)
        if read:
            return random.choice(sensor_switch_ioas)
        return random.choice(actuator_switch_ioas)