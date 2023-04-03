from pathlib import Path
from typing import IO
from threading import Thread, Event, Lock
from queue import Queue, Empty
import time
import curses
from enum import Enum


def follow_thread(logviewer):
    logpath = logviewer.log_path
    quitevent = logviewer.quit_event
    pauseevent = logviewer.pause_event
    on_content_change = logviewer.on_content_change
    logfile = logpath.open("r")

    lines = []

    while not quitevent.is_set():
        if not pauseevent.is_set():
            line = logfile.readline()
            if line:
                lines.append(line)

            if (not line and len(lines) > 0) or len(lines) > 50:
                # Push buffer
                on_content_change(lines)
                lines = []
                continue
            elif not line:
                time.sleep(0.1)
                continue
        else:
            time.sleep(0.1)


class CLR(Enum):
    WHITE = 8

PAIR_MAIN = 3
PAIR_HEADER = 2
PAIR_FOOTER = 2


class LogViewer:
    def __init__(self, logpath: Path, follow: bool):
        self.log_path = logpath
        self.quit_event = Event()
        self.pause_event = Event()
        self.lock = Lock()
        self.follow = follow
        self.scr = None
        self._log = []
        self.logfile = None

    def run(self):
        try:
            self.logfile = self.log_path.open("r")
        except Exception as e:
            self.log(f"Could not open Log File: {e=}")
            return
        curses.wrapper(self.wrapped)

    def log(self, msg: str):
        self._log.append(msg)

    def print_log(self):
        for entry in self._log:
            print(entry)

    def wrapped(self, scr):
        self.scr = scr
        self.start()
        self.stop()

    def start(self):
        curses.noecho()
        curses.cbreak()
        self.scr.keypad(True)
        self.scr.erase()
        self.scr.refresh()
        height, width = self.scr.getmaxyx()

        color_fkie = 16
        color_bg = 17
        color_fg = 18

        # Codeschool Dark + FKIE Blue
        colors = {
            "bg": [35, 44, 49],
            "font": [158, 167, 166],
            "fkie": [23, 156, 125]
        }

        curses.start_color()
        curses.use_default_colors()

        if False and curses.can_change_color() and curses.COLORS >= 16:
            c = [int((clr/255)*1000) for clr in colors["bg"]]
            self.log(f"BG: {c}")
            curses.init_color(color_bg, c[0], c[1], c[2])
            c = [int((clr/255)*1000) for clr in colors["font"]]
            self.log(f"Font: {c}")
            curses.init_color(color_fg, c[0], c[1], c[2])
            c = [int((clr/255)*1000) for clr in colors["fkie"]]
            self.log(f"FKIE: {c}")
            curses.init_color(color_fkie, c[0], c[1], c[2])

            curses.init_pair(PAIR_MAIN, curses.COLOR_WHITE, color_bg)
            curses.init_pair(PAIR_HEADER, curses.COLOR_WHITE, color_fkie)
        else:
            #curses.init_pair(PAIR_MAIN, curses.COLOR_BLACK, curses.COLOR_WHITE)
            curses.init_pair(PAIR_MAIN, -1, -1) #Use default
            curses.init_pair(PAIR_HEADER, curses.COLOR_WHITE, curses.COLOR_BLUE)

        self.header = curses.newwin(1, width, 0, 0)
        self.footer = curses.newwin(1, width, height-1, 0)

        self.update_panels()

        self.window_height = height - 2
        self.window_width = width
        #self.window_width = 2000 # Horizontal scroll

        self.line_count = 0
        self.window = curses.newpad(height - 2, width)
        self.window.scrollok(True)
        self.window.erase()
        self.scrollpos = 0

        t = Thread(target=follow_thread, args=[self])
        t.start()

        while not self.quit_event.is_set():
            ch = self.scr.getch()
            if 0 <= ch < 256 and chr(ch) == "q":
                self.quit_event.set()
            elif 0 <= ch < 256 and chr(ch) == "p":
                if self.pause_event.is_set():
                    self.pause_event.clear()
                else:
                    self.pause_event.set()
            elif ch == curses.KEY_END:
                with self.lock:
                    self.scrollpos = self.line_count
                    self.follow = True
                self.refresh()
            elif ch == curses.KEY_DOWN:
                with self.lock:
                    self.scrollpos += 1
                self.refresh()
            elif ch == curses.KEY_HOME:
                with self.lock:
                    self.scrollpos = 0
                    self.follow = False
                self.refresh()
            elif ch == curses.KEY_NPAGE:
                with self.lock:
                    self.scrollpos += self.window_height
                self.refresh()
            elif ch == curses.KEY_PPAGE:
                with self.lock:
                    self.scrollpos -= self.window_height
                    self.follow = False
                self.refresh()
            elif ch == curses.KEY_UP:
                with self.lock:
                    self.scrollpos -= 1
                    self.follow = False
                self.refresh()
            elif ch == curses.KEY_RESIZE:
                with self.lock:
                    height, width = self.scr.getmaxyx()
                    w_width = width
                    #w_width = self.window_width
                    self.update_panels()
                    self.window.resize(height - 2, w_width)
                    self.window.erase()
                self.refresh()
        t.join()

    def update_panels(self):
        height, width = self.scr.getmaxyx()
        self.header.erase()
        self.header.resize(1, width)
        self.header.mvwin(0, 0)
        self.header.bkgd(0, curses.color_pair(PAIR_HEADER))
        self.header.addstr(f"Live Log: {self.log_path.absolute()}", curses.color_pair(PAIR_HEADER))
        self.header.refresh()

        self.footer.erase()
        self.footer.resize(1, width)
        self.footer.mvwin(height - 1, 0)
        self.footer.bkgd(0, curses.color_pair(PAIR_FOOTER))
        self.set_footer()
        self.footer.refresh()

    def set_footer(self, state = ""):
        self.footer.erase()
        _, width = self.footer.getmaxyx()
        follow_active = "Active" if self.follow else "Inactive"
        self.footer.addstr(f"q: Quit     p: Pause Reading     End: Follow ({follow_active})", curses.color_pair(PAIR_FOOTER))
        self.footer.addstr(0, width - len(state) - 1, state, curses.color_pair(PAIR_FOOTER))
        self.footer.refresh()

    def on_content_change(self, lines):
        with self.lock:
            self.line_count += len(lines)
            _, width = self.window.getmaxyx()
            self.window.resize(self.line_count, width)
            for line in lines:
                line = line.rstrip().replace("\x00", "") + "\n"
                self.window.addstr(line, curses.color_pair(PAIR_MAIN))
        self.refresh()

    def refresh(self):
        with self.lock:
            height, width = self.scr.getmaxyx()
            self.window_height = height - 2
            self.window_width = width

            self.scrollpos = min(self.scrollpos, self.line_count - self.window_height - 1)
            self.scrollpos = max(0, self.scrollpos)

            self.window.bkgd(0, curses.color_pair(PAIR_MAIN))

            if self.follow:
                # Snap to top
                self.scrollpos = max(0, self.line_count - self.window_height - 1)
            max_lines = min(self.scrollpos+self.window_height+1, self.line_count)
            range = f"{self.scrollpos+1}-{max_lines} / {self.line_count}"
            self.set_footer(range)
            try:
                self.window.refresh(self.scrollpos, 0, 1, 0, height - 2, width)
            finally:
                return

    def stop(self):
        curses.nocbreak()
        self.scr.keypad(False)
        curses.echo()
        curses.endwin()
