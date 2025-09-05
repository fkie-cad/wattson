import time
from collections import deque


class UPSManager:
    def __init__(self, target_ups=0.0):
        """
        

        Args:
            target_ups:
                Target UPS (updates per seconds). Default value 0 ( do not limit the UPS)
        """
        self._events = deque(
            maxlen=int((target_ups + 1) * 5) if target_ups > 0 else 10240)

        self.up2date = True
        self.target_ups = target_ups

    def add(self):
        """
        Call to add an event to the manager (sth happened) :return:

        """

        self._events.append(time.time())
        self.up2date = False

    def current_ups(self) -> float:
        """
        :return: actual UPS value

        """
        if len(self._events) <= 1 or self._events[-1] == self._events[0]:
            current_ups = self.target_ups
        else:
            current_ups = len(self._events) / (
                    self._events[-1] - self._events[0])
        return current_ups

    def get_wait_time(self) -> float:
        """
        Determine time to wait such that the target UPS is reached.
        For now, this is only a trivial implementation: don't wait if we are "behind schedule", otherwise wait the maximum time (1 / target_UPS).
        As a result, we will always be slightly behind the schedule. Never too fast) :return: Recommended time to wait

        """
        delay = 0.0
        if not self._unlimited():
            diff = self.current_ups() - self.target_ups
            updates_ahead = diff / self.target_ups
            if diff > 0 and len(self._events) > 0:
                # optimal solution, but ignores that this takes time, too
                #       | time should have passed         | - | time that actually passed   |
                delay = len(self._events) / self.target_ups - (
                            self._events[0] - time.time())
                # delay = .5 * delay + 0.5 * (1.005 - self.current_ups() / self.target_ups) * delay
                # print(delay)
                delay = min(1 / self.target_ups, delay)
                delay = max(delay, 0)
        return delay

    def _unlimited(self):
        return self.target_ups == 0.0
