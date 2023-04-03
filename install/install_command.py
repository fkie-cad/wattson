import shutil
import sys
import time
from pathlib import Path
from typing import Any, Optional, Type
import curses

from setuptools import Command, Distribution as Distribution
import subprocess
from .component_install import ComponentInstall, InstallOperation
from .components.apt_install import AptInstall
from .components.containernet_install import ContainernetInstall
from .components.ipmininet_install import IpMininetInstall
from .components.libiec_install import LibIecInstall
from .components.ovs_install import OvsInstall
from .log_screen import LogScreen


class WattsonSetupFailedException(Exception):
    pass


class WattsonInstallDependencies(Command):
    def __init__(self, dist: Distribution, **kw: Any) -> None:
        super().__init__(dist, **kw)
        self.last_phase = None
        self.phase = "Init"
        self._step = 0
        self._phase_id = 0
        self.components = "a"
        self.install_path = "wattson-dependencies"
        self._do_apt = False
        self._do_ovs = False
        self._do_containernet = False
        self._do_libiec = False
        self._do_ipmininet = False
        self._do_cleanup = False
        self._exception = None
        self._binary_path = Path(sys.executable).resolve().absolute()
        self.path = Path(".")
        self.screen = None

    help_str = "\n".join([
        "Components to install, one letter per option.",
        "a = All (except clean); DEFAULT",
        "u = Uninstall / Clean, i.e., remove temporary files of installed components. Only affects selected components",
        "o = Open vSwitch",
        "c = Containernet",
        "i = IPMininet",
        "l = Lib IEC",
        "d = APT Dependencies",
        "E.g.: sudo python3 setup.py ubuntu --components=ociu"
    ])
    description = "Install Wattson Dependencies"
    user_options = [
        ("components=", "c", help_str),
        ("install-path=", "i", "Folder to install dependencies in"),
    ]

    def initialize_options(self):
        return

    def finalize_options(self):
        allowed_chars = "auocild"
        for char in self.components:
            if char not in allowed_chars:
                raise WattsonSetupFailedException(f"Invalid Install Option: {char}")
        if "a" in self.components:
            self._do_apt = True
            self._do_ovs = True
            self._do_containernet = True
            self._do_libiec = True
            self._do_ipmininet = True
        if "u" in self.components:
            self._do_cleanup = True
        if "d" in self.components:
            self._do_apt = True
        if "o" in self.components:
            self._do_ovs = True
        if "c" in self.components:
            self._do_containernet = True
        if "i" in self.components:
            self._do_ipmininet = True
        if "l" in self.components:
            self._do_libiec = True
        self.path = Path(self.install_path)

    def run(self):
        log_screen = LogScreen()
        try:
            with log_screen:
                wattson_task = log_screen.add_task("Installing Wattson Dependencies")
                action = InstallOperation.INSTALL
                if self._do_cleanup:
                    action = InstallOperation.UNINSTALL
                    wattson_task.set_message(f"Uninstalling Wattson Dependencies")

                apt_task = wattson_task.add_subtask("APT Dependencies")
                libiec_task = wattson_task.add_subtask("LibIEC")
                ovs_task = wattson_task.add_subtask("Open vSwitch")
                containernet_task = wattson_task.add_subtask("Containernet")
                ipmininet_task = wattson_task.add_subtask("IPMininet")

                for step_info in [
                    [apt_task, AptInstall, self._do_apt],
                    [libiec_task, LibIecInstall, self._do_libiec],
                    [ovs_task, OvsInstall, self._do_ovs],
                    [containernet_task, ContainernetInstall, self._do_containernet],
                    [ipmininet_task, IpMininetInstall, self._do_ipmininet],
                ]:
                    task = step_info[0]
                    installer_cls = step_info[1]
                    skip = not step_info[2]
                    if installer_cls is not None:
                        installer_cls: Type[ComponentInstall]
                        installer = installer_cls(self.path, task)
                        installer.prepare_tasks(action)
                    if skip:
                        task.skip()
                wattson_task.run()
                sys.exit(0)
        except Exception as e:
            wattson_task.error(repr(e))
            raise
        finally:
            log_screen.print_full_log()

    def cleanup(self):
        pass

    def install_lib_iec(self):
        self.phase = "LibIEC"
        t_dir = self.path
        # Clone Repository
        self.print(f"Cloning Library...", temporary=True)
        clone_dir = "libiec60870"
        token = "gitlab+deploy-token-51:31c_aiU3aPscf82GXj9s"
        repo = "gitlab.fit.fraunhofer.de/de.tools/libiec60870-python-bindings.git"
        if not self._exec([["git", "clone", f"https://{token}@{repo}", clone_dir]], cwd=t_dir):
            self.print("Failed to clone libiec python binding repository", success=False)
            self.print(f"{self._exception}", success=False)
            raise WattsonSetupFailedException("Could not clone libiec")
        self.print("LibIEC cloned")
        # Build and Install Library
        self.print("Installing LibIEC", temporary=True)
        t_dir = t_dir.joinpath(clone_dir)
        if not self._exec([["python3", "setup.py", "install"]], cwd=t_dir):
            self.print("Failed to build and install libiec python binding", success=False)
            self.print(f"{self._exception}", success=False)
            raise WattsonSetupFailedException("Could not install libiec")
        self.print("LibIEC installed")


    def _get_apt_conflict_packages(self):
        return ["python3-psutil", "python3-pyqt5"]

    def _get_pip_dependencies(self):
        return [
            "tftpy",  # For OVS
            "wheel", "ninja",  # For c104
            "git+https://github.com/lennart-bader/python-iptables.git"
        ]
