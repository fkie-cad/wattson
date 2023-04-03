import shutil
import sys
from pathlib import Path
from typing import Optional, List

from ..component_install import ComponentInstall, InstallOperation
from ..log_task import Task, TaskState


class LibIecInstall(ComponentInstall):
    def __init__(self, folder: Path, main_task):
        super().__init__(folder, main_task)
        self.lib_dir = "libiec"
        self.commit = "30f2f445d6159eae2e2a5d598d24fd8137aa1d49"
        self.src_dir = "lib60870-C"
        self.module_name = "iec60870"

    def get_steps(self) -> int:
        return len(self.main_task.get_subtasks())

    @property
    def prefix(self) -> str:
        return "LibIEC"

    def get_apt_dependencies(self) -> List[str]:
        return []

    def get_python_dependencies(self) -> List[str]:
        return []

    def prepare_tasks(self, operation: InstallOperation):
        if operation == InstallOperation.INSTALL:
            self.main_task.add_subtask("Cloning LibIEC", self._clone_libiec)
            self.main_task.add_subtask("Checkout LibIEC", self._checkout_libiec)
            self.main_task.add_subtask("Build LibIEC", self._build_libiec)
        elif operation == InstallOperation.UNINSTALL:
            #self.main_task.add_subtask("Uninstall LibIEC", self._uninstall_libiec)
            self.main_task.add_subtask("Cleaning Folders", self.clean)
        self.prepared = True

    def install(self) -> bool:
        if not self.prepared:
            self.prepare_tasks(InstallOperation.INSTALL)
        return self.run_tasks()

    def uninstall(self) -> bool:
        self.journal = {
            "installed": False
        }
        self.save_journal()
        if not self.prepared:
            self.prepare_tasks(InstallOperation.UNINSTALL)
        return self.run_tasks()

    def _uninstall_libiec(self, task: Task):
        commands = [
            [sys.executable, "-m", "pip", "uninstall", self.module_name]
        ]
        if not self.exec(commands, task=task):
            task.failed()
            return
        task.success()

    def _clone_libiec(self, task):
        t_dir = self.path
        if t_dir.joinpath(self.lib_dir).is_dir():
            task.info(f"Folder already exists")
            task.skip_done()
            return
        task.info(f"Cloning into {t_dir.joinpath(self.lib_dir)}")
        token = "gitlab+deploy-token-51:31c_aiU3aPscf82GXj9s"
        repo = "gitlab.fit.fraunhofer.de/de.tools/libiec60870-python-bindings.git"
        if not self.exec(
            [["git", "clone", f"https://{token}@{repo}", self.lib_dir]],
            task=task,
            cwd=t_dir.absolute()
        ):
            task.error("Failed to clone LibIEC")
            task.failed()
        else:
            task.info("Cloned LibIEC")
            task.success()

    def _checkout_libiec(self, task):
        t_dir = self.path.joinpath(self.lib_dir)
        task.info(f"Checking out {self.commit}")
        if not self.exec(
            [["git", "checkout", self.commit]],
            task=task,
            cwd=t_dir.absolute()
        ):
            task.error("Failed to checkout target commit")
            task.failed()
        else:
            task.success()

    def _build_libiec(self, task):
        t_dir = self.path.joinpath(self.lib_dir).joinpath(self.src_dir)
        commands = [
            [sys.executable, "setup.py", "install"]
        ]

        if not self.exec(commands, task=task, cwd=t_dir):
            task.failed()
            return
        task.success()
