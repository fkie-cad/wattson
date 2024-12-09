import shutil
import subprocess
from pathlib import Path
from typing import Optional, List

from ..component_install import ComponentInstall, InstallOperation
from ..log_task import Task, TaskState


class AptInstall(ComponentInstall):
    def __init__(self, folder: Path, main_task):
        super().__init__(folder, main_task)
        self.from_step = 0
        self.linux_headers = None
        self.is_windows = False

    def get_steps(self) -> int:
        return len(self.main_task.get_subtasks())

    @property
    def prefix(self) -> str:
        return "APT"

    def get_kernel_version(self, task):
        if not self.exec([["uname", "-r"]], task=task):
            task.warning(f"Could not extract Linux Headers Version (uname -r)")
            task.warning(f"You might have to install them yourself")
            task.failed()
        else:
            uname = self._last_stdout[0]
            self.linux_headers = f"linux-headers-{uname}"
            if "microsoft" in self.linux_headers:
                self.linux_headers = None
                self.is_windows = True
            task.success()

    def get_apt_dependencies(self) -> List[str]:
        packages = [
           "git", "python3", "python3-pip", "python3-dev", "swig", "gcc", "make", "cmake", "build-essential",
           "net-tools", "libcap-ng-utils", "libcap-ng-dev", "libssl-dev", "libexpat-dev", "bison",
           "help2man", "libssl-dev", "python3-zope.interface", "python3-twisted", "xterm",
           "socat", "gfortran", "libopenblas-dev", "liblapack-dev", "ninja-build", "graphviz",
           "libgraphviz-dev", "graphviz-dev", "pkg-config", "docker.io", "python3-pyqt5",
            "dpkg-dev", "lintian", "devscripts", "fakeroot", "debhelper", "dh-autoreconf", "uuid-runtime",
           "autoconf", "automake", "libtool", "python3-all", "dh-python", "xdg-utils", "groff", "netcat-openbsd", "curl",
           "ethtool", "libunbound-dev", "libunbound8", "libcap-ng-dev", "libssl-dev", "openssl",
           "python3-pyftpdlib", "qt6-base-dev", "qt6-webengine-dev", "qt6-webview-dev",
           "python3-flake8", "lftp", "swig", "dsniff", "dbus-x11", "openvswitch-switch",
            "frr", "frr-pythontools"
        ]
        if self.linux_headers is not None:
            packages.append(self.linux_headers.strip())
        return packages

    def get_python_dependencies(self) -> List[str]:
        return [
            "wheel", "ninja",  # For c104
            "git+https://github.com/lennart-bader/python-iptables.git"
        ]

    def get_apt_removals(self) -> List[str]:
        return ["python3-psutil"]

    def prepare_tasks(self, operation: InstallOperation):
        if operation == InstallOperation.INSTALL:
            kernel_task = self.main_task.add_subtask("Extracting Kernel Version", self.get_kernel_version)
            kernel_task.required = False
            update_task = self.main_task.add_subtask(f"Updating Packages", self.apt_update)
            update_task.required = False

            def uninstall(task):
                self.uninstall_apt(self.get_apt_removals(), task)

            self.main_task.add_subtask(f"Uninstalling conflicting Packages", uninstall)
            self.main_task.add_subtask("Installing APT Dependencies", self.install_apt)
            self.main_task.add_subtask("Installing PIP Dependencies", self.install_pip)
            self.main_task.add_subtask("Setting up FRRouting", self._fix_frrouting)
        elif operation == InstallOperation.UNINSTALL:
            self.main_task.skip_done()
            self.main_task.warning(f"Wattson won't uninstall your APT packages")
        self.prepared = True

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
