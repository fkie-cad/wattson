import shlex
from typing import Union, List


class NestedArgument:
    def __init__(self, argument: Union[List[str], str], is_nested: bool = False):
        self.is_nested = is_nested
        self._argument = argument

    def __str__(self):
        if isinstance(self._argument, str):
            parts = [self._argument]
        else:
            parts = self._argument
        escaped = []
        for part in parts:
            if isinstance(part, str):
                escaped.append(part)
            elif isinstance(part, List):
                escaped.append(str(NestedArgument(part, True)))
            elif isinstance(part, NestedArgument):
                part.is_nested = True
                escaped.append(str(part))
        result = ' '.join(escaped)
        if self.is_nested:
            return shlex.quote(result)
        return result
