from threading import Thread, Event
import zmq
from queue import Queue, Empty

from wattson.util import get_logger


class ZMQWrapper(Thread):
    """ ZMQ worker-thread for sending & rcving cmds in a REQ-RESP fashion """
    def __init__(self, context: zmq.Context, tasks: Queue, sock_info: str, **kwargs):
        """

        Args:
            context: zmqContext
            tasks: Queue of tasks to be executed
            **kwargs:
                - sock_info
                - send_timeout_s: int (2) timeout (seconds) for sending stuff (0 = forever)
                - recv_timeout_s: int (8) timeout 9seconds) for rcving a reply (0 = forever wait)
        """
        super().__init__()
        self.sock_info = sock_info
        self.send_timeout_s = kwargs.get('send_timeout_s', 2)   # 0 = forever
        self.recv_timeout_s = kwargs.get('recv_timeout_s', 8)   # 0 = forever
        self.context = context
        self.tasks = tasks
        self.logger = get_logger("unknown", "ZMQ Logger")
        self._block_delay_s = 0.5
        self._terminate = Event()
        self.socket = None

    def start(self) -> None:
        try:
            self.socket = self.context.socket(zmq.REQ)
            self.socket.setsockopt(zmq.LINGER, self.send_timeout_s * 1000)
            self.socket.connect(self.sock_info)
            super().start()
        except zmq.ZmqBindError as e:
            self.logger.error(f"Failed to connect to ZMQ command Socket {e=}")
            print("Connected ZMQ")
        # super().start()

    def stop(self):
        self._terminate.set()

    def run(self) -> None:
        """
        Periodically waits for new msg to be send, waiting for reply and stores it if "block" attribute is set.
        """
        while not self._terminate.is_set():
            try:
                task = self.tasks.get(True, self._block_delay_s)
            except Empty:
                continue
            try:
                json = task["json"]
                self.socket.send_string(json)
                if task["block"]:
                    if self.socket.poll(self.recv_timeout_s * 1000):
                        reply = self.socket.recv_json()
                    else:
                        raise TimeoutError(
                            f"Server did not reply in {self.recv_timeout_s}s to msg {json}"
                        )
                    task["reply"] = reply
                    task["on_reply"].set()
                elif "on_reply" in task:
                    task["on_reply"].set()
                else:
                    self.logger.critical("No on_reply given")

            except (TimeoutError, zmq.ZMQError) as e:
                print(f"Could not send/recv {e}")
                self.logger.critical(f"Could not send/recv {e}")

