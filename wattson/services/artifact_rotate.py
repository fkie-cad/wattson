from pathlib import Path
from typing import Any, Callable, Optional


class ArtifactRotate:
    def __init__(self, file_name: Path, on_rotate_callback: Optional[Callable[['ArtifactRotate'], Any]] = None, is_folder: bool = False):
        self._history = []
        self._base_name = file_name
        self._is_folder = is_folder
        self._on_rotate_callback = on_rotate_callback

    def set_rotate_callback(self, on_rotate_callback: Optional[Callable[['ArtifactRotate'], Any]] = None):
        self._on_rotate_callback = on_rotate_callback

    def rotate(self):
        """
        Rotates all existing artifacts by one position and optionally calls the on_rotate_callback
        :return:
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
            self._history[i] = file_path.rename(self._base_name.parent.joinpath(f"{self._base_name.name}.{i}"))
        if self._on_rotate_callback is not None:
            self._on_rotate_callback(self)

    def get_current(self) -> Path:
        if len(self._history) == 0:
            self.rotate()
        return self._history[0]
