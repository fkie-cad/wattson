import ctypes
import sys
import trace
import threading


class InterruptableThread(threading.Thread):
    def start(self):
        self.killed = False
        self.__original_run = self.run
        self.run = self.__run
        threading.Thread.start(self)

    def __run(self):
        sys.settrace(self.globaltrace)
        self.__original_run()
        self.run = self.__original_run

    def globaltrace(self, frame, event, arg):
        if event == "call":
            return self.localtrace
        return None

    def localtrace(self, frame, event, arg):
        if self.killed:
            if event == "line":
                raise SystemExit()
        return self.localtrace

    def get_id(self):
        return self.ident

    def kill(self):
        self.killed = True

    def interrupt(self) -> bool:
        thread_id = self.get_id()
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(thread_id), ctypes.py_object(SystemExit))
        if res > 1:
            ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, 0)
            return False
        return True
