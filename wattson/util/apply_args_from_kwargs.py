from typing import List, Dict, Union, Tuple

from wattson.util.custom_exceptions import MissingArgumentError, ArgumentIsNoneError


def apply_args_from_kwargs(self,
                           args: Union[List[str], Tuple[str, ...], Dict[str, str]],
                           **kwargs):
    if isinstance(args, list):
        args = {arg: arg for arg in args}

    for arg in args:
        if arg not in kwargs:
            raise MissingArgumentError(f"Missing required argument {arg}")
        if kwargs[arg] is None:
            raise ArgumentIsNoneError(f"{arg} in {kwargs}")
        assert kwargs[arg] is not None
        self.__setattr__(args[arg], kwargs[arg])


