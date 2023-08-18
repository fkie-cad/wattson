from logging import Logger
from typing import Optional, Callable

import zmq
from threading import Thread, Event
from queue import Queue, Empty

from zmq.utils.monitor import recv_monitor_message

from wattson.util import get_logger

from wattson.services.deployment.cli.messages import CLIMessage, PrintMessage, CommandMessage, AutoCompleteMessage, SystemMessage
from wattson.services.deployment.cli.messages import SystemMessageType

class CLIServer(Thread):
    def __init__(self, ip: str = "*", port: int = 61195, cmd_callback: Optional[Callable] = None,
                 auto_complete_callback: Optional[Callable] = None, system_callback: Optional[Callable] = None,
                 logger: Optional[Logger] = None):
        super().__init__()

        if logger is None:
            self._logger: Logger = get_logger("CLI-Server", "CLI-Server")
        else:
            self._logger: Logger = logger
        self.ip = ip
        self.port = port
        self.cmd_callback = cmd_callback
        self.auto_complete_callback = auto_complete_callback
        self.system_callback = system_callback

        self.connection_string = None
        self.running: bool = False
        self.server = None
        self.monitor = None
        self._context = zmq.Context()
        self._stop_requested = Event()
        self._connect_event = Event()
        self._disconnect_event = Event()
        self._read_queue = Queue()
        self._write_queue = Queue()

    def start(self):
        try:
            self.server = self._context.socket(zmq.PAIR)
            self.monitor = self.server.get_monitor_socket()
            monitor_thread = Thread(target=self.event_monitor, args=(self.monitor,))
            monitor_thread.start()
            self.connection_string = f"tcp://{self.ip}:{self.port}"
            self.server.bind(self.connection_string)
            self._logger.info(f"Binding to {self.connection_string}")
            super(CLIServer, self).start()
        except Exception as e:
            self._logger.error(f"Failed to bind: {e}")
            self.stop()

    def stop(self):
        self._stop_requested.set()

    def run(self):
        while not self._stop_requested.is_set() or not self._write_queue.empty():
            # Read all available messages
            while True:
                try:
                    message_str = self.server.recv_string(flags=zmq.NOBLOCK)
                    self._logger.info(message_str)
                    message = CLIMessage.from_str(message_str)
                    message.interface = self
                    add = True
                    if isinstance(message, CommandMessage) and self.cmd_callback is not None:
                        add = self.cmd_callback(message)
                    if isinstance(message, AutoCompleteMessage) and self.auto_complete_callback is not None:
                        add = self.auto_complete_callback(message)
                    if isinstance(message, SystemMessage) and self.system_callback is not None:
                        add = self.system_callback(message)

                    if add:
                        self._read_queue.put(message)
                except zmq.ZMQError:
                    break
            # Write all available messages
            while True:
                try:
                    message = self._write_queue.get(False)
                    self.server.send_string(message.to_json())
                except zmq.ZMQError:
                    self._logger.error("ZMQ Send-Queue is full. Message could not be sent.")
                except Empty:
                    break


        disconnect = SystemMessage.factory(SystemMessageType.DISCONNECT)
        self.server.send_string(disconnect.to_json())
        self._logger.info("Stopping CLI Server")
        self.server.disconnect(self.connection_string)

    def read_available(self):
        return not self._read_queue.empty()

    def read(self, block: bool = True, timeout: Optional[float] = None) -> Optional[CLIMessage]:
        try:
            return self._read_queue.get(block=block, timeout=timeout)
        except Empty:
            return None

    def print(self, message, print_format: str = "plain", follow_prompt: bool = False):
        msg = PrintMessage()
        msg.format = print_format
        msg.data = message
        msg.follow_prompt = follow_prompt
        self.send(msg)

    def send(self, message: CLIMessage):
        if not self._disconnect_event.is_set():
            self._write_queue.put(message)

    def send_unblock(self):
        self.send_sys(SystemMessageType.ENABLE_PROMPT)

    def send_block(self):
        self.send_sys(SystemMessageType.DISABLE_PROMPT)

    def send_ok(self):
        self.send_sys(SystemMessageType.OK)

    def send_sys(self, sys_type: SystemMessageType):
        self.send(SystemMessage.factory(sys_type))

    def wait_for_disconnect(self, timeout: Optional[float] = None):
        self._disconnect_event.wait(timeout=timeout)

    def event_monitor(self, monitor):
        EVENT_MAP = {}
        for name in dir(zmq):
            if name.startswith('EVENT_'):
                value = getattr(zmq, name)
                EVENT_MAP[value] = name

        while monitor.poll():
            evt = recv_monitor_message(monitor)
            evt.update({'description': EVENT_MAP[evt['event']]})
            if evt["event"] == zmq.EVENT_HANDSHAKE_SUCCEEDED:
                self._connect_event.set()
                self._disconnect_event.clear()
            if evt["event"] == zmq.EVENT_DISCONNECTED:
                self._connect_event.clear()
                self._disconnect_event.set()
            self._logger.info(f"Event: {evt}")
        monitor.close()

    def wait_for_connect(self, timeout: Optional[float] = None):
        self._connect_event.wait(timeout=timeout)
