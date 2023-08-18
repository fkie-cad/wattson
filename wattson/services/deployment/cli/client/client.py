from queue import Queue, Empty
from logging import Logger
from threading import Thread, Event
from typing import Optional, Callable
import zmq
from zmq.utils.monitor import recv_monitor_message

from wattson.services.deployment.cli.messages import CLIMessage, CommandMessage, AutoCompleteMessage, PrintMessage, SystemMessage
from wattson.util import get_logger


class CLIClient(Thread):
    def __init__(self, server_ip: str, server_port: int = 61195, print_callback: Optional[Callable] = None,
                 auto_complete_callback: Optional[Callable] = None, system_callback: Optional[Callable] = None,
                 logger: Optional[Logger] = None):
        super().__init__()
        self.ip = server_ip
        self.port = server_port
        self.print_callback = print_callback
        self.auto_complete_callback = auto_complete_callback
        self.system_callback = system_callback
        self._logger = get_logger("CLIClient", "CLIClient", use_context_logger=False) if logger is None else logger
        self._stop_requested = Event()
        self._connect_event = Event()
        self.monitor_thread = None
        self._read_queue = Queue()
        self._write_queue = Queue()
        self._requested_message = None
        self.connection_string = None

        self.context = zmq.Context()
        self.socket = None

    def start(self, server_ip: Optional[str] = None, server_port: Optional[int] = None):
        if server_ip is not None:
            self.ip = server_ip
        if server_port is not None:
            self.port = server_port
        self.connection_string = f"tcp://{self.ip}:{self.port}"
        try:
            self.socket = self.context.socket(zmq.PAIR)
            self.monitor = self.socket.get_monitor_socket()
            self.monitor_thread = Thread(target=self.event_monitor, args=(self.monitor,))
            self.monitor_thread.start()

            self.socket.connect(self.connection_string)
            self._logger.debug(f"Connecting to {self.ip}")
            super().start()
        except Exception as e:
            self._logger.error(f"Failed to connect: {e}")
            self.stop()

    def event_monitor(self, monitor):
        EVENT_MAP = {}
        for name in dir(zmq):
            if name.startswith('EVENT_'):
                value = getattr(zmq, name)
                EVENT_MAP[value] = name

        while monitor.poll():
            evt = recv_monitor_message(monitor)
            evt.update({'description': EVENT_MAP[evt['event']]})
            if evt["event"] == zmq.EVENT_CONNECTED:
                self._connect_event.set()
            if evt["event"] == zmq.EVENT_DISCONNECTED:
                self._connect_event.clear()
            if evt["event"] == zmq.EVENT_MONITOR_STOPPED:
                break
            self._logger.debug(f"Event: {evt}")
        monitor.close()

    def stop(self):
        self._stop_requested.set()

    def run(self) -> None:
        while not self._stop_requested.is_set():
            # Read all available messages
            while True:
                try:
                    message_str = self.socket.recv_string(flags=zmq.NOBLOCK)
                    message = CLIMessage.from_str(message_str)
                    message.interface = self
                    add = True
                    if isinstance(message, PrintMessage) and self.print_callback is not None:
                        add = self.print_callback(message)
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
                    self.socket.send_string(message.to_json())
                except zmq.ZMQError:
                    self._logger.error("ZMQ Send-Queue is full. Message could not be sent.")
                    break
                except Empty:
                    break

        self._logger.debug("Stopping CLI Client")
        self.socket.close()
        self.monitor_thread.join()
        #self.socket.disconnect(self.connection_string)
        self._logger.debug("Stopped CLI Client")

    def request_auto_complete(self, partial_string_cmd: str, block: bool = True, timeout: float = 3) \
            -> Optional[AutoCompleteMessage]:
        message = AutoCompleteMessage()
        message.request = CommandMessage()
        message.request.string_cmd = partial_string_cmd
        original_auto_complete_callback = self.auto_complete_callback
        message_there_event = Event()
        self._requested_message = None

        def _temporary_callback(auto_complete_message):
            if auto_complete_message.id == message.id:
                self._requested_message = auto_complete_message
                message_there_event.set()
            if original_auto_complete_callback is not None:
                return original_auto_complete_callback(auto_complete_message)
            return True

        if block:
            self.auto_complete_callback = _temporary_callback

        self.send(message)

        if block:
            message_there_event.wait(timeout)
            self.auto_complete_callback = original_auto_complete_callback
            return self._requested_message
        return None

    def read_available(self):
        return not self._read_queue.empty()

    def read(self, block: bool = True, timeout: Optional[float] = None) -> Optional[CLIMessage]:
        try:
            return self._read_queue.get(block=block, timeout=timeout)
        except Empty:
            return None

    def cmd(self, command_string: str):
        msg = CommandMessage()
        msg.string_cmd = command_string
        self.send(msg)

    def send(self, message: CLIMessage):
        self._write_queue.put(message)

    def wait_connect(self, timeout: Optional[float] = None):
        self._connect_event.wait(timeout)

    def is_connected(self):
        return self._connect_event.is_set()

