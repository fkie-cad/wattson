import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional, List

from ..component_install import ComponentInstall, InstallOperation
from ..log_task import Task, TaskState


class IpMininetInstall(ComponentInstall):
    def __init__(self, folder: Path, main_task):
        super().__init__(folder, main_task)
        self.ip_mininet_dir = "ipmininet"
        self.tmp_dir = "ipmininet-tmp"

    def get_steps(self) -> int:
        return len(self.main_task.get_subtasks())

    @property
    def prefix(self) -> str:
        return "IPMininet"

    def get_apt_dependencies(self) -> List[str]:
        return [
            "cgroup-tools", "git", "autoconf", "automake", "libtool", "make", "gawk", "libreadline-dev", "texinfo",
            "pkg-config", "libpam0g-dev", "libjson-c-dev", "bison", "flex", "python3-pytest",
            "libc-ares-dev", "python3-dev", "libsystemd-dev", "python3-sphinx", "install-info",
            "build-essential", "libsystemd-dev", "libcap-dev",
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

    def _fix_docker_key(self, task: Task):
        cmd = " ".join([
            "curl", "-fsSL", "https://download.docker.com/linux/ubuntu/gpg", "|",
            "gpg", "--dearmor", "|",
            "tee", "/usr/share/keyrings/docker-ce-archive-keyring.gpg"
        ])
        if not self.exec([cmd], task=task, shell=True):
            task.failed()
        else:
            task.success()

    def _clean_folders(self, task: Task):
        opt_path = Path("/opt/mininet-dependencies")
        tmp_path = self.path.joinpath(self.tmp_dir)
        paths = [
            self.path.joinpath(self.ip_mininet_dir),
            tmp_path,
            opt_path
        ]
        for p in paths:
            try:
                task.info(f"Removing {str(p)}")
                shutil.rmtree(p, ignore_errors=True)
            except Exception as e:
                task.error(f"Removal failed: {e=}")
                task.failed()
                return
        task.info(f"Creating {opt_path}")
        opt_path.mkdir(parents=True, exist_ok=True)
        task.info(f"Creating {tmp_path}")
        tmp_path.mkdir(parents=True, exist_ok=True)

    def _clone(self, task: Task):
        t_dir = self.path.joinpath(self.ip_mininet_dir)
        if t_dir.is_dir():
            task.info(f"Directory already exists - skipping clone")
        elif not self.exec([["git", "clone", "--depth", "1", "--branch", "v1.1",
                             "https://github.com/cnp3/ipmininet.git", self.ip_mininet_dir]],
                           cwd=self.path, task=task):
            task.error(f"Could not clone IPMininet")
            task.failed()
            return
        task.success()

    def _patch(self, task: Task):
        cmds = [
            # Disable Mininet installation
            ["sed", "-i", "s/setup_mininet_dep()$/#setup_mininet_dep()/", "setup.py"],
            # Disable Python2 Dependencies
            ["sed", "-i", "s/os.remove(link)/os.unlink(link)/", "ipmininet/install/install.py"],
            ["sed", "-i", "s/os.path.exists(link)/os.path.islink(link)/", "ipmininet/install/install.py"]
            # sed -i "s/require_cmd(cls.NAME/require_cmd(cls.NAME[0]/", "ipmininet/router/config/base.py"
        ]
        t_dir = self.path.joinpath(self.ip_mininet_dir)
        for file in t_dir.joinpath("ipmininet/install").glob("*openr-rc*.sh"):
            cmds.append(["sed", "-e", "s/python-setuptools//g", "-i", f"ipmininet/install/{file.name}"])
            cmds.append(["sed", "-e", "s/python-pip//g", "-i", f"ipmininet/install/{file.name}"])

        cmd_tasks = []
        for cmd in cmds:
            t = task.add_subtask(" ".join(cmd))
            cmd_tasks.append((cmd, t))

        for info in cmd_tasks:
            cmd = info[0]
            t = info[1]
            t.start()
            if not self.exec([cmd], cwd=t_dir, task=task):
                task.error(f"Patching failed")
                t.failed()
                task.failed()
                return
            else:
                t.success()
        task.success()

    def _build(self, task: Task):
        t_deps = task.add_subtask(f"Installing IPMininet Dependencies")
        t_ip = task.add_subtask(f"Installing IPMininet")
        cmd = [sys.executable, "-m", "ipmininet.install", "-r", "-o", self.tmp_dir]
        t_deps.start()
        if not self.exec([cmd], task=t_deps, cwd=self.path.joinpath(self.ip_mininet_dir)):
            t_deps.failed()
            task.failed()
            return
        t_deps.success()

        cmd = [sys.executable, "-m", "pip", "install", "--no-deps", "."]
        t_ip.start()
        if not self.exec([cmd], task=t_ip, cwd=self.path.joinpath(self.ip_mininet_dir)):
            t_ip.failed()
            task.failed()
            return
        t_ip.success()
        task.success()

    def install(self) -> bool:
        if not self.prepared:
            self.prepare_tasks(InstallOperation.INSTALL)
        return self.run_tasks()

    def uninstall(self) -> bool:
        if not self.prepared:
            self.prepare_tasks(InstallOperation.UNINSTALL)
        return self.run_tasks()
