import shutil
import subprocess
from pathlib import Path
from typing import Optional, List

from ..component_install import ComponentInstall, InstallOperation
from ..log_task import Task, TaskState


class ContainernetInstall(ComponentInstall):
    def __init__(self, folder: Path, main_task):
        super().__init__(folder, main_task)
        self.cnet_dir = "containernet"
        self.cnet_commit = "38205d51c78b2acceb4cc4113532fd80a2868685"
        self.from_step = 0

    def get_steps(self) -> int:
        return len(self.main_task.get_subtasks())

    @property
    def prefix(self) -> str:
        return "Containernet"

    def get_apt_dependencies(self) -> List[str]:
        return ["ansible", "aptitude", "cgroup-tools", "python3-scapy"]

    def get_python_dependencies(self) -> List[str]:
        return []

    def prepare_tasks(self, operation: InstallOperation):
        if operation == InstallOperation.INSTALL:
            update_task = self.main_task.add_subtask(f"Updating Packages", self.apt_update)
            update_task.required = False
            self.main_task.add_subtask("Installing APT Dependencies", self.install_apt)
            self.main_task.add_subtask("Cloning Containernet", self._clone)
            self.main_task.add_subtask("Patching", self._patch)
            self.main_task.add_subtask("Preparing Build", self._clear_build)
            self.main_task.add_subtask("Build", self._build)

        elif operation == InstallOperation.UNINSTALL:
            self.main_task.add_subtask("Removing Containernet Folder", self._remove_folder)
        self.prepared = True

    def _clone(self, task: Task):
        t_dir = self.path.joinpath(self.cnet_dir)
        if t_dir.is_dir():
            task.info(f"Directory already exists - skipping clone")
        elif not self.exec([
            ["git", "clone", "https://github.com/containernet/containernet.git", self.cnet_dir]
        ], cwd=self.path, task=task):
            task.error(f"Could not clone Containernet")
            task.failed()
            return

        if not self.exec([["git", "checkout", self.cnet_commit]], cwd=t_dir, task=task):
            task.error(f"Could not checkout requested Commit")
            task.failed()
        else:
            task.success()

    def _patch(self, task: Task):
        cmds = [
            # Patch for Ubuntu 22.04
            ["sed", "-i", "s/cgroup-bin/cgroup-tools/g", "util/install.sh"],
            ["sed", "-i", "s/python-scapy/python3-scapy/g", "util/install.sh"],
            ["sed", "-i", "s/pyflakes /pyflakes3 /g", "util/install.sh"],
            # Replace SSH with https Cloning for Github
            ["sed", "-i", "s/git:\/\/github/https:\/\/github/g", "util/install.sh"],
            # Freeze python-iptables
            ["sed", "-i", "/python-iptables/d", "setup.py"]
        ]
        t_dir = self.path.joinpath(self.cnet_dir)
        cmd_tasks = []
        for cmd in cmds:
            t = task.add_subtask(" ".join(cmd))
            cmd_tasks.append((cmd, t))

        ansible_file = t_dir.joinpath("ansible/install.yml")
        task.add_subtask("Patching Mininet Ansible Install", on_run=self.find_and_replace_task(
            file=ansible_file,
            search='util/install.sh',
            replace='util/install.sh -knfwp',
            condition=lambda x: "-knfwp" not in x
        ))
        task.add_subtask("Fixing IP Overriding", on_run=self.find_and_replace_task(
            file=t_dir.joinpath("mininet/link.py"),
            search="overwrite=True",
            replace="overwrite=False"
        ))

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

        task.run()

    def _clear_build(self, task: Task):
        t_dir = self.path
        for build_dir in ["openflow", "pox", "oftest", "oflops", "ofsoftswitch13", "loxigen",
                          "ivs", "ryu", "noxcore", "nox13oflib"]:
            build_path = t_dir.joinpath(build_dir)
            if build_path.is_dir():
                try:
                    shutil.rmtree(build_path)
                    task.info(f"Removed {build_dir}")
                except Exception as e:
                    task.error(f"Could not remove {build_dir}")
                    task.failed()
                    return
        task.success()

    def _build(self, task: Task):
        ansible_dir = self.path.joinpath(self.cnet_dir).joinpath("ansible")
        if not self.exec(
                [['ansible-playbook', '-i', 'localhost,', '-c', 'local', 'install.yml']],
                cwd=ansible_dir, task=task):
            task.error(f"Failed to build Containernet")
            task.failed()
            return
        task.success()

    def _remove_folder(self, task: Task):
        t_dir = self.path.joinpath(self.cnet_dir)
        task.info(f"Removing {t_dir}")
        if t_dir.is_dir():
            try:
                shutil.rmtree(t_dir)
                task.success()
            except Exception as e:
                task.error(f"Failed to clean Containernet Artefacts: {e=}")
                task.failed()
        else:
            task.info(f"Folder does not exist")
            task.skip_done()

    def install(self) -> bool:
        if not self.prepared:
            self.prepare_tasks(InstallOperation.INSTALL)
        return self.run_tasks()

    def uninstall(self) -> bool:
        if not self.prepared:
            self.prepare_tasks(InstallOperation.UNINSTALL)
        return self.run_tasks()
