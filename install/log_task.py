import curses
import dataclasses
import enum
import sys
import textwrap
import time
from typing import Optional, TYPE_CHECKING, List, Tuple, Union, Callable

if TYPE_CHECKING:
    from log_screen import LogScreen


class TaskState(enum.Enum):
    QUEUED = 0
    ACTIVE = 1
    DONE = 2
    FAILED = 3
    DELETED = 4
    DONE_SKIPPED = 5
    SKIPPED = 6
    FAIL_SKIPPED = 7


class LogLevel(enum.Enum):
    DEBUG = 0
    INFO = 1
    WARNING = 2
    ERROR = 3
    CRITICAL = 4


@dataclasses.dataclass
class LogLine:
    text: str
    level: LogLevel

    _wrapped: Optional[List[str]] = None
    _wrapped_len: int = -1

    def wrap(self, line_length: int):
        if self._wrapped is None or line_length != self._wrapped_len:
            try:
                self._wrapped = textwrap.wrap(self.text, width=line_length)
                self._wrapped_len = line_length
            except Exception as e:
                print(repr(self.text), file=sys.stderr)
                print(repr(e), file=sys.stderr)
                raise

    @property
    def wrapped(self) -> List[str]:
        return self._wrapped

    @property
    def color(self):
        return {
            LogLevel.WARNING: 2,
            LogLevel.ERROR: 1,
            LogLevel.CRITICAL: 1,
        }.get(self.level)


class Task:
    def __init__(self, msg: str, state: TaskState = TaskState.QUEUED, level: int = 0):
        self._state = state
        self._msg = msg
        self._screen: Optional['LogScreen'] = None
        self._level = level
        self._id = -1
        self._parent_task: Optional['Task'] = None
        self._sub_tasks: List['Task'] = []
        self._log: List[LogLine] = []
        self.on_run = None
        self.required = True
        self._block_on_change = False

    def set_parent(self, parent: 'Task'):
        self._parent_task = parent

    def add_subtask(self, task: Union['Task', str], on_run: Optional[Callable] = None):
        if type(task) == str:
            task = Task(task)
        task.level = self._level + 1
        task.parent = self
        if on_run is not None:
            task.on_run = on_run
        self._sub_tasks.append(task)
        return task

    def run(self):
        if self.state != TaskState.QUEUED:
            return
        self.start()
        if self.on_run is not None:
            self.on_run(self)
        if self.state not in [TaskState.ACTIVE, TaskState.DONE]:
            return
        if self.state != TaskState.ACTIVE and len(self._sub_tasks) > 0:
            self.start()
        skip_remaining = False
        for sub_task in self._sub_tasks:
            if skip_remaining:
                if sub_task.state == TaskState.QUEUED:
                    sub_task.skip_fail()
                continue
            if sub_task.state == TaskState.QUEUED:
                sub_task.run()
            if sub_task.state not in [TaskState.DONE, TaskState.DONE_SKIPPED, TaskState.SKIPPED] and sub_task.required:
                self.failed()
                skip_remaining = True
        if self.state == TaskState.ACTIVE:
            self.success()

    def info(self, text: str):
        self.log(text=text, level=LogLevel.INFO)

    def warning(self, text: str):
        self.log(text=text, level=LogLevel.WARNING)

    def error(self, text: str):
        self.log(text=text, level=LogLevel.ERROR)

    def log(self, text: str, level: LogLevel = LogLevel.INFO):
        self._log.append(LogLine(text=text, level=level))
        self._on_change()

    def set_message(self, msg: str):
        self._msg = msg
        self._on_change()

    @property
    def message(self):
        return self._msg

    def get_lines(self, max_line_length: int = -1) -> Tuple[List[str], List[LogLine]]:
        lines = []
        if max_line_length > 0:
            lines.append(self._msg)
        else:
            lines.extend(textwrap.wrap(self._msg, width=max_line_length))
        for log_line in self._log:
            log_line.wrap(max_line_length)
        return lines, self._log

    def get_state_icon(self) -> str:
        icons = {
            TaskState.QUEUED: "⧖",
            TaskState.ACTIVE: "|/-\\",  # "»",
            TaskState.DONE: "✓",
            TaskState.FAILED: "×", #"⚠",
            TaskState.DELETED: "%",
            TaskState.SKIPPED: "»",
            TaskState.DONE_SKIPPED: "»",
            TaskState.FAIL_SKIPPED: "!",

        }.get(self.state)
        if len(icons) == 1:
            return icons
        speed = 2
        t = time.time() * speed
        pos = int(t % len(icons))
        return icons[pos]

    def get_state_color(self):
        return {
            TaskState.QUEUED: 5,
            TaskState.ACTIVE: 3,
            TaskState.DONE: 4,
            TaskState.FAILED: 1,
            TaskState.DELETED: 5,
            TaskState.DONE_SKIPPED: 4,
            TaskState.SKIPPED: None,
            TaskState.FAIL_SKIPPED: 2
        }.get(self.state)

    @property
    def parent(self):
        return self._parent_task

    @parent.setter
    def parent(self, _parent_task: 'Task'):
        self._parent_task = _parent_task

    @property
    def level(self):
        return self._level

    @level.setter
    def level(self, _level):
        self._level = _level

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, _state: TaskState):
        self.set_state(_state)

    @state.deleter
    def state(self):
        self.set_state(TaskState.DELETED)
        self._on_change()

    def get_subtasks(self, filtered: bool = False) -> List['Task']:
        if filtered:
            return [task for task in self._sub_tasks if task.state != TaskState.DELETED]
        return self._sub_tasks

    def get_id(self) -> int:
        return self._id

    def set_id(self, _id: int):
        self._id = _id

    def link_screen(self, log_screen: 'LogScreen'):
        self._screen = log_screen

    def set_state(self, state: TaskState):
        self._state = state
        self._on_change()
        return self

    def start(self):
        return self.set_state(TaskState.ACTIVE)

    def skip_done(self, recursive: bool = True):
        if recursive:
            self._block_on_change = True
            for task in self._sub_tasks:
                if task.state == TaskState.QUEUED:
                    task.skip_done(recursive)
            self._block_on_change = False
        return self.set_state(TaskState.DONE_SKIPPED)

    def skip(self, recursive: bool = True):
        if recursive:
            self._block_on_change = True
            for task in self._sub_tasks:
                if task.state == TaskState.QUEUED:
                    task.skip(recursive)
            self._block_on_change = False
        return self.set_state(TaskState.SKIPPED)

    def skip_fail(self, recursive: bool = True):
        if recursive:
            self._block_on_change = True
            for task in self._sub_tasks:
                if task.state == TaskState.QUEUED:
                    task.skip_fail(recursive)
            self._block_on_change = False
        return self.set_state(TaskState.FAIL_SKIPPED)

    def success(self):
        return self.set_state(TaskState.DONE)

    def failed(self, recursive_skip: bool = True):
        if recursive_skip:
            self._block_on_change = True
            for task in self._sub_tasks:
                if task.state == TaskState.QUEUED:
                    task.skip_fail(recursive_skip)
            self._block_on_change = False
        return self.set_state(TaskState.FAILED)

    def queue(self):
        return self.set_state(TaskState.QUEUED)

    def delete(self):
        return self.set_state(TaskState.DELETED)

    def _on_change(self):
        if self._block_on_change:
            return
        if self._parent_task is not None:
            self._parent_task._on_change()
        elif self._screen is not None:
            self._notify_screen()

    def _notify_screen(self):
        self._screen.task_updated(self)
