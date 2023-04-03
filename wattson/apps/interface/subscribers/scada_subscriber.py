from itertools import product
import logging
import random
import threading as th
import time
from time import sleep
from typing import Optional
import multiprocessing as mp

from wattson.util import ContextLogger, get_logger

from wattson.apps.interface.util import messages as msgs
from wattson.apps.interface.clients.combi_client import CombiClient
from wattson.apps.interface.clients.reference_type import ReferenceType
from wattson.apps.interface.util.constants import DEFAULT_PUB_SERVER_IP, DEFAULT_CMD_SERVER_PORT, \
    DEFAULT_PUB_SERVER_PORT, NO_RESP
from wattson.apps.interface.util.confirmation_status import ConfirmationStatus

from wattson.iec104.interface.types import TypeID, COT


class ScadaSubscriber(mp.Process):
    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
        ups: float = 0.05,
        **kwargs,
    ):
        super().__init__()
        self.mtu_ip = kwargs.get('mtu_ip', DEFAULT_PUB_SERVER_IP)
        self.mtu_cmd_port = kwargs.get('mtu_cmd_port', DEFAULT_CMD_SERVER_PORT)
        self.mtu_pub_port = kwargs.get('mtu_sub_port', DEFAULT_PUB_SERVER_PORT)

        self.node_id = kwargs.get('node_id', 'ScadaSub')
        if logger is not None:
            self.logger = logger.getChild(self.node_id)
        else:
            self.logger = get_logger(self.node_id, "wattson.MTU")
        self.mtu_client = CombiClient(self.node_id, self.logger, log_name="CombiClient",
                                      mtu_ip=self.mtu_ip,
                                      mtu_cmd_port=self.mtu_cmd_port, mtu_sub_port=self.mtu_pub_port,
                                      on_cmd_update=self.on_cmd_update, on_cmd_reply=self.on_cmd_reply,
                                      on_update=self.on_general_update, on_dp_update=self.on_dp_update)
        #self.mtu_client.logger.setLevel(logging.WARNING)
        self.logger.setLevel(logging.INFO)
        self.always_valid = kwargs.get('always_valid', True)
        self._terminate = th.Event()
        self.wait_t = 1 / ups

        self.actuator_switch_ioas = [35110, 31110, 32110, 33110, 34110]
        self.sensor_switch_ioas = [25110, 21110, 22110, 23110, 24110]
        self.sensor_ioas = [10010 + i * 1000 + j * 10 for (i, j) in product(range(6), range(4))]
        self.sensor_ioas.remove(10020)
        self.bad_ioas = [0, 1000, 2345, 2345981, 1202002023]

    def run(self):
        self.mtu_client.start()
        last_update = 0
        time.sleep(3)
        while not self._terminate.is_set():
            if time.time() > last_update + self.wait_t:
                # adds tells command to send of the message as soon as possible
                #control_ioa = random.choice(self.actuator_switch_ioas)
                #write_msg, read_msg = self.write_read_cmd(control_ioa)
                #self.mtu_client.send_cmd(write_msg)
                #self.mtu_client.send_cmd(read_msg)
                msg = self.random_cmd()
                self.mtu_client.send_cmd(msg)
                last_update = time.time()
            else:
                sleep(self.wait_t)

    def on_dp_update(self, update: msgs.ProcessInfoMonitoring, orig_msg: Optional[msgs.ReadDatapoint] = None):
        if update.cot not in (COT.PERIODIC, COT.INTERROGATED_BY_STATION):
            self.logger.info(f"Received non-periodic, non-interro dp-update with vals {update.val_map} and ts {update.ts_map}")

    def on_cmd_update(self, msg: msgs.IECMsg, orig_msg: msgs.IECMsg):
        """ Handles Status-update of a prior-send command, e.g., as ACT_TERM """
        if isinstance(msg, msgs.ProcessInfoMonitoring):
            info = f"Result of requested datapoints through command {orig_msg} for RTU {msg.coa}:"
            for ioa, val in msg.val_map.items():
                info += f" {ioa} -> {val}"
            self.logger.info(info)
        elif isinstance(msg, msgs.Confirmation):
            self.logger.warning(f"Received msg {msg}")
            # doesn't parse status correctly
            if msg.result["status"] == ConfirmationStatus.POSITIVE_CONFIRMATION.value:
                self.logger.warning(f"RTU confirmed requested change from prior msg {orig_msg}.")
                ctrl_ioa = next(iter(orig_msg.val_map.keys()))
                _, read_msg = self.write_read_cmd(ctrl_ioa)
                self.mtu_client.send_cmd(read_msg)
                self.logger.warning(f"Sending read-msg")
            elif msg.result["status"] == ConfirmationStatus.FAIL.value:
                self.logger.warning(f"RTU denied requested change from prior msg {orig_msg} with reason {msg.result['reason']}.")
            elif msg.result["status"] == ConfirmationStatus.SUCCESSFUL_TERM.value:
                self.logger.debug(f"RTU ended update from prior msg {orig_msg}")
        else:
            self.logger.debug(f"Other update {msg} for msg {orig_msg}")

    def on_cmd_reply(self, reply: msgs.IECMsg, orig_msg: msgs.IECMsg):
        """ Handles first reply to a msg send to the CommandServer """
        if reply != NO_RESP:
            if isinstance(reply, msgs.Confirmation) and reply.result["status"] == "Failed":
                self.logger.warning(f"MTU denied msg {orig_msg} with reason {reply.result['reason']}.")
                self.mtu_client.delete_send_msg(orig_msg.reference_nr)
            elif isinstance(reply, msgs.TotalInterroReply):
                self.logger.info(f"Total Interro result: {reply}")
            elif isinstance(reply, msgs.RTUStatusReply):
                self.logger.debug(f"Current RTU sconn status: {reply}")

    def on_general_update(self, msg: msgs.IECMsg, _: ReferenceType):
        if isinstance(msg, msgs.Confirmation):
            self.logger.debug(f"MTU updated information about prior msg to through confirmation {msg}.")
        else:
            self.logger.debug(f"MTU informed about general update through msg {msg}.")

    def stop(self):
        self.mtu_client.stop()
        self._terminate.set()

    def random_cmd(self, coa=163):
        # Selects which message to send
        cmd = random.randint(0, 10)
        if cmd < 6:
            # will create a C_SC or C_RD subscription-message
            return self.switch_command(coa)
        elif cmd == 6:
            return self.C_CS(coa)
        elif cmd == 7:
            return self.C_IC(coa)
        elif cmd == 8:
            return self.change_cyclic_transmission(coa)
        elif cmd == 9:
            # will receive general Information from the MTU:
            # RTUs' connection status, datapoints
            self.logger.info("Interrogating MTU.")
            return msgs.TotalInterroReq()
        else:
            self.logger.info("Requesting overall RTU connection status.")
            return msgs.RTUStatusReq()

    def write_read_cmd(self, control_ioa: int = 0):
        contr_val = random.choice((True, False))
        contr_ioa = random.choice(self.actuator_switch_ioas) if not control_ioa else control_ioa
        read_ioa = contr_ioa - 10000
        write_msg = msgs.ProcessInfoControl(
            163, TypeID.C_SC_NA_1, {contr_ioa: contr_val}
        )
        read_msg = msgs.ReadDatapoint(163, read_ioa)
        return write_msg, read_msg

    def C_CS(self, coa=163):
        msg = msgs.SysInfoControl(TypeID.C_CS_NA_1, coa, COT.ACTIVATION)
        return msg

    def C_IC(self, coa=163):
        msg = msgs.SysInfoControl(TypeID.C_IC_NA_1, coa, COT.ACTIVATION)
        return msg

    def change_cyclic_transmission(self, coa=163):
        valid_ioa = random.choice((True, False))
        activate = random.choice((True, False))
        sensor_ioas = [10010 + i * 1000 + j * 10 for (i, j) in product(range(6), range(4))]
        sensor_ioas.remove(10020)
        ioa = random.choice(sensor_ioas)
        msg = msgs.ParameterActivate(coa, ioa, activate)
        return msg

    def switch_command(self, coa=163):
        read = random.choice((True, False))
        # sensor IOAs are from datapoints with a type in [1, 40]
        # actuator IOAs are from datapoints with a type in [45, 69]
        valid_ioa = random.choice((True, False)) or self.always_valid
        ioa = self.command_ioa(valid_ioa, read)

        if read:
            msg = msgs.ReadDatapoint(coa, ioa)
        else:
            valid_type = random.choice((True, False)) or self.always_valid
            control_type_ID = self.command_type(valid_type)
            # type of corresponding datapoint (Here IEC104 Single Command)
            control_val = random.choice((True, False))
            control_val2 = not control_val
            ioa2 = self.command_ioa(valid_ioa, False)
            msg = msgs.ProcessInfoControl(
                coa,
                control_type_ID,
                {ioa: control_val, ioa2: control_val2},
            )
        return msg

    #@staticmethod
    def command_ioa(self, valid: bool, read: bool):
        if not valid:
            return random.choice(self.bad_ioas)
        elif read:
            return random.choice(self.sensor_switch_ioas + self.sensor_ioas)
        return random.choice(self.actuator_switch_ioas)

    def command_type(self, valid: bool):
        bad_types = [TypeID.P_AC_NA_1, TypeID.M_SP_NA_1, TypeID.C_DC_NA_1]

        if not valid:
            return random.choice(bad_types)
        return TypeID.C_SC_NA_1