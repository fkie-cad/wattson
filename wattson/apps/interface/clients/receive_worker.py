from threading import Thread, Event
from queue import Queue, Empty
from typing import Callable


class ReceiveWorker(Thread):
    def __init__(self, queue: Queue, handler: Callable):
        super().__init__()
        self.queue = queue
        self.handler = handler
        self._terminate: Event = Event()

    def start(self):
        super().start()

    def stop(self):
        self._terminate.set()

    def run(self):
        while not self._terminate.is_set():
            try:
                msg = self.queue.get(True, 2)
                self.handler(msg)
            except Empty:
                continue

