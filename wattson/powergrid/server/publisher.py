import queue
import threading

import zmq


class Publisher(threading.Thread):
    def __init__(self, server_address):
        super().__init__()
        self.queue = queue.Queue()
        self.server_address = server_address
        self._stopped = threading.Event()
        self._poll_time = 1

    def run(self):
        with zmq.Context() as ctx:
            with ctx.socket(zmq.PUB) as sock:
                sock.bind(self.server_address)
                while not self._stopped.is_set():
                    try:
                        msg = self.queue.get(timeout=self._poll_time)
                    except queue.Empty:
                        # no message to send..
                        continue
                    sock.send_pyobj(msg)
                    self.queue.task_done()

    def stop(self):
        self._stopped.set()

    def send_msg(self, msg):
        self.queue.put(msg)
