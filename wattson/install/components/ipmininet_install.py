import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional, List

from ..component_install import ComponentInstall, InstallOperation
from ..log_task import Task, TaskState


class FRRoutingInstall(ComponentInstall):
    def __init__(self, folder: Path, main_task):
        super().__init__(folder, main_task)

    def get_steps(self) -> int:
        return len(self.main_task.get_subtasks())

    @property
    def prefix(self) -> str:
        return "FRRouting"

    def get_apt_dependencies(self) -> List[str]:
        return [
            "frr", "frr-pythontools"
        ]

    def get_python_dependencies(self) -> List[str]:
        return ["mako==1.1"]

    def get_apt_removals(self) -> List[str]:
        return []

    def prepare_tasks(self, operation: InstallOperation):
        if operation == InstallOperation.INSTALL:
            def uninstall(task):
                self.uninstall_apt(self.get_apt_removals(), task)

            self.main_task.add_subtask("Fixing Docker APT Key", self._fix_docker_key)
            update_task = self.main_task.add_subtask(f"Updating Packages", self.apt_update)
            update_task.required = False
            self.main_task.add_subtask("Installing APT Dependencies", self.install_apt)
            self.main_task.add_subtask("Installing PIP Dependencies", self.install_pip)
            self.main_task.add_subtask("Removing APT Conflicts", uninstall)
            self.main_task.add_subtask("Cleaning Directories", self._clean_folders)
            self.main_task.add_subtask("Cloning IPMininet", self._clone)
            self.main_task.add_subtask("Fix FRRouting", self._fix_frrouting)
            self.main_task.add_subtask("Patching Install Scripts", self._patch)
            self.main_task.add_subtask("Building IPMininet", self._build)

        elif operation == InstallOperation.UNINSTALL:
            self.main_task.add_subtask("Uninstalling IPMininet", self._uninstall_pip)
            self.main_task.add_subtask("Removing IPMinenet Folders", self._clean_folders)
        self.prepared = True

    def _uninstall_pip(self, task: Task):
        cmd = [sys.executable, "-m", "pip", "uninstall", "ipmininet"]
        if not self.exec([cmd], task=task):
            task.failed()
            return
        task.success()

    def _fix_frrouting(self, task: Task):
        if not Path("/usr/sbin/ospfd").exists():
            cmd = ["ln", "-s", "/usr/lib/frr/ospfd", "/usr/sbin/ospfd"]
            if not self.exec([cmd], task=task):
                task.failed()
                return
        if not Path("/usr/sbin/zebra").exists():
            cmd = ["ln", "-s", "/usr/lib/frr/zebra", "/usr/sbin/zebra"]
            if not self.exec([cmd], task=task):
                task.failed()
                return
        cmd = ["usermod", "-aG", "frrvty", "root"]
        if not self.exec([cmd], task=task):
            task.failed()
            return
        task.success()

    def install(self) -> bool:
        if not self.prepared:
            self.prepare_tasks(InstallOperation.INSTALL)
        return self.run_tasks()

    def uninstall(self) -> bool:
        if not self.prepared:
            self.prepare_tasks(InstallOperation.UNINSTALL)
        return self.run_tasks()
