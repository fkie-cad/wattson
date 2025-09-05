from pathlib import Path
from typing import Any, Callable, Optional


class ArtifactRotate:
    def __init__(self, file_name: Path, on_rotate_callback: Optional[Callable[['ArtifactRotate'], Any]] = None,
                 is_folder: bool = False, detect_existing: bool = True):
        self._history = []
        self._base_name = file_name
        self._is_folder = is_folder
        self._on_rotate_callback = on_rotate_callback
        if detect_existing:
            self._restore_history()

    def get_base_name(self) -> str:
        return self._base_name.name

    def is_empty(self) -> bool:
        if self._is_folder:
            return not any(self.get_current().iterdir())
        return (not self.get_current().exists()) or self.get_current().stat().st_size == 0

    def _generate_filename(self, index: int = 0):
        if index == 0:
            return self._base_name.parent.joinpath(f"{self._base_name.name}")
        else:
            return self._base_name.parent.joinpath(f"{self._base_name.name}.{index}")

    def _restore_history(self):
        file_name = self._generate_filename(0)
        i = 0
        while file_name.exists():
            self._history.append(file_name)
            i += 1
            file_name = self._generate_filename(i)

    def set_rotate_callback(self, on_rotate_callback: Optional[Callable[['ArtifactRotate'], Any]] = None):
        self._on_rotate_callback = on_rotate_callback

    def rotate(self):
        """
        Rotates all existing artifacts by one position and optionally calls the on_rotate_callback :return:

        """
        self._history.insert(0, self._base_name)
        if self._is_folder:
            self._base_name.mkdir(exist_ok=True)
        else:
            self._base_name.touch(exist_ok=True)
        for i in reversed(range(len(self._history))):
            if i == 0:
                continue
            file_path = self._history[i]
            self._history[i] = file_path.rename(self._generate_filename(i))
        if self._on_rotate_callback is not None:
            self._on_rotate_callback(self)

    def get_current(self) -> Path:
        if len(self._history) == 0:
            self.rotate()
        return self._history[0]
