import sys
from pathlib import Path
from typing import Any, Type

from setuptools import Command, Distribution as Distribution
from .component_install import ComponentInstall, InstallOperation
from .components.apt_install import AptInstall
from .components.containernet_install import ContainernetInstall
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
        self._do_containernet = False
        self._do_cleanup = False
        self._exception = None
        self._binary_path = Path(sys.executable).resolve().absolute()
        self.path = Path(".")
        self.screen = None

    help_str = "\n".join([
        "Components to install, one letter per option.",
        "a = All (except clean); DEFAULT",
        "u = Uninstall / Clean, i.e., remove temporary files of installed components. Only affects selected components",
        "c = Containernet",
        "d = APT Dependencies",
        "E.g.: sudo python3 setup.py ubuntu --components=oiu"
    ])
    description = "Install Wattson Dependencies"
    user_options = [
        ("components=", "c", help_str),
        ("install-path=", "i", "Folder to install dependencies in"),
    ]

    def initialize_options(self):
        return

    def finalize_options(self):
        allowed_chars = "aucid"
        for char in self.components:
            if char not in allowed_chars:
                raise WattsonSetupFailedException(f"Invalid Install Option: {char}")
        if "a" in self.components:
            self._do_apt = True
        if "u" in self.components:
            self._do_cleanup = True
        if "d" in self.components:
            self._do_apt = True
        if "c" in self.components:
            self._do_containernet = True
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
                containernet_task = wattson_task.add_subtask("Containernet")

                for step_info in [
                    [apt_task, AptInstall, self._do_apt],
                    [containernet_task, ContainernetInstall, self._do_containernet],
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

    def _get_apt_conflict_packages(self):
        return ["python3-psutil", "python3-pyqt5"]

    def _get_pip_dependencies(self):
        return [
            "wheel", "ninja",  # For c104
            "git+https://github.com/lennart-bader/python-iptables.git"
        ]
