import shutil
from pathlib import Path
from typing import Optional, List

from ..component_install import ComponentInstall, InstallOperation
from ..log_task import Task, TaskState


class OvsInstall(ComponentInstall):
    def __init__(self, folder: Path, main_task):
        super().__init__(folder, main_task)
        self.ovs_dir = "ovs"
        self.from_step = 0
        self.service_target_file = Path("/etc/systemd/system/wattson-ovs.service")

    def get_steps(self) -> int:
        return len(self.main_task.get_subtasks())

    @property
    def prefix(self) -> str:
        return "OVS"

    def get_apt_dependencies(self) -> List[str]:
        return ["autoconf", "make", "libtool"]

    def get_python_dependencies(self) -> List[str]:
        return ["tftpy"]

    def prepare_tasks(self, operation: InstallOperation):
        if operation == InstallOperation.INSTALL:
            apt_task = self.main_task.add_subtask("Installing APT Dependencies", self.install_apt)
            pip_task = self.main_task.add_subtask("Installing PIP Dependencies", self.install_pip)

            self.main_task.add_subtask("Cloning OVS", self._clone_ovs)
            self.main_task.add_subtask("Patching OVS Controller", self._patch_controller)
            self.main_task.add_subtask("Building OVS", self._build_ovs)
            self.main_task.add_subtask("Preparing Service", self._prepare_service)
            self.main_task.add_subtask("Installing Service", self._install_service)
        elif operation == InstallOperation.UNINSTALL:
            rm_task = self.main_task.add_subtask("Uninstalling OVS", self._remove_ovs)
            rm_task.required = False
            self.main_task.add_subtask("Cleaning Build Folders", self.clean)
            self.main_task.add_subtask("Uninstalling Services", self._remove_service)
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

    def _remove_ovs(self, task: Task):
        t_dir = self.path.joinpath(self.ovs_dir)
        commands = [
            ["make", "uninstall"],
            ["make", "clean"]
        ]

        sub_tasks = []
        for cmd in commands:
            cmd_t = " ".join(cmd)
            sub_tasks.append(task.add_subtask(cmd_t))

        for i, cmd in enumerate(commands):
            sub_task = sub_tasks[i]
            sub_task.start()
            if not self.exec([cmd], task=sub_task, cwd=t_dir):
                sub_task.error(f"{self._exception}")
                sub_task.failed()
                return
            sub_task.success()
        task.info(f"Uninstalled OVS")
        task.success()

    def _remove_service(self, task: Task):
        if not self.exec([
            ["systemctl", "--no-pager", "daemon-reload"],
            ["systemctl", "--no-pager", "reset-failed"],
            ["systemctl", "--no-pager", "list-units", "--full", "-all", "-t", "service", "--no-legend"]
        ], task=task, cmd_silent=True, out_silent=True):
            task.parent.failed(True)
            return
        services = self.last_stdout
        found = any(["wattson-ovs.service" in line for line in services])
        if found:
            if not self.exec([
                ["systemctl", "stop", "--no-pager", "wattson-ovs"],
                ["systemctl", "disable", "--no-pager", "wattson-ovs"]
            ], task=task, shell=False):
                task.warning(f"Could not disable Wattson OVS Service")
                task.failed()
        else:
            task.info(f"Service not installed")
            task.skip_done()
        task.info("Deleting Service Configuration File")
        self.service_target_file.unlink(missing_ok=True)
        if task.state != TaskState.FAILED:
            task.success()

    def _clone_ovs(self, task):
        t_dir = self.path
        if t_dir.joinpath(self.ovs_dir).is_dir():
            task.info(f"Folder already exists")
            task.skip_done()
            return
        task.info(f"Cloning into {t_dir.joinpath(self.ovs_dir)}")
        if not self.exec(
                [["git", "clone", "-b", "v2.17.0", "https://github.com/openvswitch/ovs.git", self.ovs_dir]],
                task=task,
                cwd=t_dir.absolute()
        ):
            task.error("Failed to clone OVS")
            task.failed()
        else:
            task.info("Cloned OVS")
            task.success()

    def _patch_controller(self, task):
        # Patch Test Controller
        t_dir = self.path.joinpath(self.ovs_dir)
        file = t_dir.joinpath("utilities/ovs-testcontroller.c")
        try:
            with file.open("r+") as f:
                lines = f.readlines()
                for i in range(len(lines)):
                    line = lines[i]
                    if line.startswith("#define MAX_SWITCHES"):
                        line = "#define MAX_SWITCHES 4096\n"
                        lines[i] = line
                    elif line.startswith("#define MAX_LISTENERS"):
                        line = "#define MAX_LISTENERS 4096\n"
                        lines[i] = line
                    elif "OpenFlow controller" in line and "Wattson" not in line:
                        line = line.replace("controller", "controller, patched for Wattson")
                        lines[i] = line
                f.seek(0)
                f.writelines(lines)
        except Exception as e:
            task.error(f"Failed patching Test-Controller")
            task.error(f"{e}")
            task.failed()
        else:
            task.info(f"Patched Test-Controller")
            task.success()

    def _build_ovs(self, task):
        t_dir = self.path.joinpath(self.ovs_dir)
        commands = [
            ["./boot.sh"],
            ["./configure"],
            ["make"],
            ["make", "install"],
            ["make", "modules_install"]
        ]

        sub_tasks = []
        for cmd in commands:
            cmd_t = " ".join(cmd)
            sub_tasks.append(task.add_subtask(cmd_t))

        for i, cmd in enumerate(commands):
            sub_task = sub_tasks[i]
            sub_task.start()
            if not self.exec([cmd], task=sub_task, cwd=t_dir):
                sub_task.error(f"{self._exception}")
                sub_task.failed()
                return
            sub_task.success()
        task.info(f"Built OVS")
        task.success()

    def _prepare_service(self, task):
        source_file = Path(__file__).parent.joinpath("wattson-ovs.service")
        try:
            shutil.copyfile(source_file, self.service_target_file)
            task.info("Copied Wattson-OVS Service Configuration")
        except Exception as e:
            task.error(f"Failed copying Wattson-OVS Service configuration")
            task.error(f"{e}")
            task.failed()
        else:
            task.success()

    def _install_service(self, task):
        if not self.exec([
            ["systemctl", "--no-pager", "daemon-reload"],
            ["systemctl", "--no-pager", "enable", "wattson-ovs"],
            ["systemctl", "--no-pager", "start", "wattson-ovs"]
        ], task=task):
            task.error(f"Failed Setting up Wattson-OVS Service")
            task.error(f"{self._exception}")
            task.failed()
        else:
            task.info(f"Set up Wattson-OVS Service")
            task.success()
