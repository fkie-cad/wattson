from threading import Thread, Event


class ResettableTimer(Thread):
    def __init__(self, interval, function, *args, **kwargs):
        Thread.__init__(self)
        self.interval = interval
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.finished = Event()
        self.resetted = True

    def cancel(self):
        """Stop the timer if it hasn't finished yet"""
        self.finished.set()

    def run(self):
        while self.resetted:
            self.resetted = False
            self.finished.wait(self.interval)

        if not self.finished.is_set():
            self.function(*self.args, **self.kwargs)
        self.finished.set()

    def reset(self, interval=None):
        """
        Reset the timer

        Args:
            interval:
                
                (Default value = None)
        """
        if interval:
            self.interval = interval
        self.resetted = True
        self.finished.set()
        self.finished.clear()
