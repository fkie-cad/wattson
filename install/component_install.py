import abc
import enum
import json
import shutil
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional, List
from .log_task import Task, TaskState


class InstallOperation(enum.Enum):
    INSTALL = 1
    UNINSTALL = 2


class ComponentInstall(abc.ABC):
    def __init__(self, folder: Path, main_task):
        self.folder = folder
        self.main_task: Task = main_task
        self.step = 0
        self.from_step = 0
        self._exception: Optional[Exception] = None
        self.from_step = 0
        self.folder.mkdir(parents=True, exist_ok=True)
        self.path = self.folder.joinpath(self.prefix)
        self.path.mkdir(parents=True, exist_ok=True)
        self.journal_file = self.folder.joinpath(f".{self.prefix}.install.jnl")
        self.journal = {}
        self._tasks = []
        if self.journal_file.exists():
            with self.journal_file.open("r") as f:
                self.journal = json.load(f)
        self._prepared = False
        self._last_stdout = []
        self._last_stderr = []

    @property
    def prepared(self):
        return self._prepared

    @prepared.setter
    def prepared(self, _prepared):
        self._prepared = _prepared

    @property
    @abc.abstractmethod
    def prefix(self) -> str:
        ...

    def inc_step(self):
        self.journal["last_step"] = self.step
        self.step += 1
        self.journal["active_step"] = self.step
        self.save_journal()

    def save_journal(self):
        with self.journal_file.open("w") as f:
            json.dump(self.journal, f)

    def step_enabled(self):
        return self.from_step <= self.step

    @property
    def last_stdout(self):
        return self._last_stdout

    @property
    def last_stderr(self):
        return self._last_stderr

    def exec(self, cmds, task: Optional[Task] = None, cmd_silent=False, out_silent=False, err_silent=False, **kwargs):
        def log_task(proc, file, callback, local_store):
            while True:
                line = file.readline()
                if not line:
                    if proc.poll() is not None:
                        return
                    continue
                try:
                    t = line.decode("utf-8")
                except UnicodeDecodeError as e:
                    t = repr(line)
                local_store.append(t)
                if callback is not None:
                    callback(t)

        if "cwd" in kwargs:
            cwd = kwargs.get("cwd")
            if isinstance(cwd, Path):
                kwargs["cwd"] = str(cwd.absolute())
        try:
            self._last_stdout = []
            self._last_stderr = []

            for cmd in cmds:
                if not cmd_silent and task is not None:
                    cmd_str = cmd if type(cmd) == str else " ".join(cmd)
                    task.info(f"Running {cmd_str}")
                with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs) as p:
                    with ThreadPoolExecutor(2) as pool:
                        info = None
                        err = None
                        if task is not None:
                            info = None if out_silent else task.info
                            err = None if err_silent else task.error
                        r1 = pool.submit(log_task, p, p.stdout, info, self._last_stdout)
                        r2 = pool.submit(log_task, p, p.stderr, err, self._last_stderr)
                        r1.result()
                        r2.result()
                    if p.returncode != 0 and task is not None:
                        task.warning(f"Process exited with status code {p.returncode}")
                        return False
        except subprocess.CalledProcessError as e:
            self._exception = e
            return False
        except FileNotFoundError as e:
            self._exception = e
            return False
        self._exception = None
        return True

    @abc.abstractmethod
    def get_steps(self) -> int:
        ...

    @abc.abstractmethod
    def get_apt_dependencies(self) -> List[str]:
        ...

    @abc.abstractmethod
    def get_python_dependencies(self) -> List[str]:
        ...

    @abc.abstractmethod
    def prepare_tasks(self, operation: InstallOperation):
        ...

    def apt_update(self, task):
        if not self.exec([["apt-get", "update"]], task=task):
            task.warning(f"Could update packages")
            task.failed()
        else:
            task.success()

    def install_apt(self, task: Optional[Task] = None):
        apt_deps = self.get_apt_dependencies()
        if len(apt_deps) > 0:
            cmd = ["apt-get", "install", "-y"] + apt_deps
            if not self.exec([cmd], task=task):
                raise Exception(f"Failed to install apt dependencies")
        elif task is not None:
            task.skip_done()

    def uninstall_apt(self, packages: List[str], task: Optional[Task] = None):
        if len(packages) > 0:
            cmd = ["apt-get", "remove", "-y"] + packages
            if not self.exec([cmd], task=task):
                raise Exception(f"Failed to uninstall apt packages")
        elif task is not None:
            task.skip_done()

    def install_pip(self, task: Optional[Task] = None):
        pip_deps = self.get_python_dependencies()
        if len(pip_deps) > 0:
            binary = sys.executable
            cmd = [binary, "-m", "pip", "install"] + pip_deps
            if not self.exec([cmd], task=task):
                raise Exception(f"Failed to install PIP dependencies")
        elif task is not None:
            task.skip_done()

    def find_and_replace_task(self, file, search, replace, condition=None):
        def apply(task):
            try:
                with file.open("r") as f:
                    content = f.read()
                if condition is not None and not condition(content):
                    task.skip_done()
                    return
                content = content.replace(search, replace)
                with file.open("w") as f:
                    f.write(content)
                task.success()
            except:
                task.failed()
        return apply

    def run_tasks(self):
        skip_remaining = False
        for task in self.main_task.get_subtasks():
            if not skip_remaining:
                task.run()
                if task.state not in [TaskState.DONE, TaskState.DONE_SKIPPED]:
                    skip_remaining = True
            else:
                task.skip()
        if skip_remaining:
            self.main_task.failed()
            return False
        self.main_task.success()
        return True

    @abc.abstractmethod
    def install(self) -> bool:
        ...

    @abc.abstractmethod
    def uninstall(self) -> bool:
        ...

    def clean(self, task):
        try:
            shutil.rmtree(self.path)
            task.info(f"Removed build files")
            task.success()
        except Exception as e:
            task.error(str(e))
            task.failed()
