import curses
import dataclasses
import enum
import os
import textwrap
import threading
from typing import Optional, Union, Dict, List, Tuple, Any

from install.log_task import Task, TaskState


class LogScreen:
    def __init__(self):
        self._entries = {}
        self._entry_id = 0
        self._tasks: Dict[int, Task] = {}
        self._log = []
        self._initialized = 0
        self._window = None
        self.state_lines = 10
        self._lock = threading.RLock()
        self._stop_requested = threading.Event()
        self._loop_thread = threading.Thread(target=self._run_loop)
        self._scroll_position = 0
        self._follow_mode = True
        self._quit_requested = threading.Event()
        self._last_line_length = 30

    def __enter__(self):
        if self._initialized == 0:
            self._init_screen()
            self._stop_requested.clear()
            self._loop_thread.start()
        self._initialized += 1

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._initialized -= 1
        if self._initialized == 0:
            self._stop_requested.set()
            self._loop_thread.join()
            self._clear_screen()

    def _run_loop(self):
        while not self._stop_requested.is_set():
            ch = self._window.getch()
            if ch == curses.ERR:
                pass
            elif ch == curses.KEY_RESIZE:
                pass
            elif ch == curses.KEY_UP:
                with self._lock:
                    if self._scroll_position > 0:
                        self._scroll_position = self._scroll_position - 1
                    self._follow_mode = False
            elif ch == curses.KEY_DOWN:
                with self._lock:
                    self._scroll_position = self._scroll_position + 1
            elif ch == curses.KEY_HOME:
                self._scroll_position = 0
                self._follow_mode = False
            elif ch == curses.KEY_END:
                self._follow_mode = True
            elif ch == ord('q'):
                self._quit_requested.set()
            self.print_tasks()

    def print_full_log(self):
        lines = self._get_task_lines_recursively(list(self._tasks.values()), indent=0, full=True)
        for line in lines:
            FAIL = '\033[91m'
            GREEN = '\033[92m'
            WARNING = '\033[93m'
            ENDC = '\033[0m'
            t = line[0].rstrip()
            clr = line[1]
            if clr is not None:
                clr = {
                    1: FAIL,
                    2: WARNING,
                    4: GREEN
                }.get(clr)

            if clr is not None:
                t = f"{clr}{t}{ENDC}"
            print(t)

    def wait_for_quit(self):
        self._quit_requested.clear()
        self._quit_requested.wait()

    def _init_screen(self):
        self._window = curses.initscr()
        self._window.scrollok(True)
        self._window.keypad(True)
        curses.curs_set(0)
        curses.noecho()
        curses.halfdelay(5)
        curses.start_color()
        curses.use_default_colors()
        curses.init_pair(1, curses.COLOR_RED, -1)
        curses.init_pair(2, curses.COLOR_YELLOW, -1)
        curses.init_pair(3, curses.COLOR_BLUE, -1)
        curses.init_pair(4, curses.COLOR_GREEN, -1)
        curses.init_pair(5, -1, -1)

    def _clear_screen(self):
        curses.echo()
        curses.nocbreak()
        curses.endwin()

    def add_task(self, task: Union[str, Task]) -> Task:
        if type(task) == str:
            task = Task(msg=task)
        if task.get_id() < 0:
            self._entry_id += 1
            task.set_id(self._entry_id)
            task.link_screen(self)
        self._tasks[task.get_id()] = task
        self.print_tasks()
        return task

    def task_updated(self, task: Task):
        self.print_tasks()

    def clear_log(self):
        self._tasks = {}
        self.print_tasks()

    def print_tasks(self):
        if self._initialized == 0:
            return False
        self._window.clear()
        lines = self._get_task_lines_recursively(list(self._tasks.values()), indent=0)
        total_lines = len(lines)
        max_y, max_x = self._window.getmaxyx()
        top_line = max(0, total_lines - max_y)
        with self._lock:
            first_line = self._scroll_position
            if first_line >= top_line or self._follow_mode:
                self._follow_mode = True
                first_line = top_line
                self._scroll_position = top_line
        for i, line in enumerate(lines[first_line:first_line+max_y]):
            text = line[0]
            clr = line[1]
            if clr is not None:
                clr = curses.color_pair(clr)
                self._window.addstr(i, 0, text, clr)
            else:
                self._window.addstr(i, 0, text)

        self._window.refresh()

    def _get_task_lines_recursively(self, tasks: List[Task], indent: int,
                                    level: int = 0, line_length: Optional[int] = None,
                                    full: bool = False) -> List[Tuple[str, Any]]:
        if line_length is None and self._initialized > 0:
            y, x = self._window.getmaxyx()
            self._last_line_length = x

        line_length = self._last_line_length
        max_log_lines = 10
        lines = []

        space_indent = indent + 4

        def p_line(text: str, color):
            lines.append((" "*space_indent + text, color))

        for task in tasks:
            if task.state == TaskState.DELETED:
                continue
            task_lines, log_lines = task.get_lines(line_length - space_indent)
            task_icon = task.get_state_icon()
            task_color = task.get_state_color()
            for i, t_line in enumerate(task_lines):
                if i == 0:
                    line = " "*indent + f"[{task_icon}] " + t_line
                    lines.append((line, task_color))
                else:
                    p_line(t_line, task_color)
            if full:
                for log_line in log_lines:
                    for line in log_line.wrapped:
                        p_line(line, log_line.color)
            elif task.state in [TaskState.ACTIVE, TaskState.FAILED]:
                # Long version
                potential_lines = []
                for log_line in log_lines[-max_log_lines:]:
                    for line in log_line.wrapped:
                        potential_lines.append((line, log_line.color))
                for log_line in potential_lines[-max_log_lines:]:
                    p_line(log_line[0], log_line[1])
            # Sub-Tasks
            sub_indent = indent + 2
            lines.extend(self._get_task_lines_recursively(task.get_subtasks(), indent=sub_indent, full=full))
        return lines
