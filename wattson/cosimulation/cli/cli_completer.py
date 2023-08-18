import readline
import sys
import traceback
from typing import List, Optional


class CLICompleter:
    def __init__(self, cli):
        self.cli = cli
        self.logic = {}

    def setup(self):
        self._build_logic()

    def complete(self, text, state):
        try:
            tokens = readline.get_line_buffer().split()
            if not tokens or readline.get_line_buffer()[-1] == ' ':
                tokens.append("")
            candidates = self._traverse(tokens, self.logic)
            options = []
            single_option = len(candidates) == 1
            names = list(c[0] for c in candidates)
            max_length = max([len(name) for name in names] + [0])
            for name, d in candidates:
                name: str
                if not single_option and d.get("description") is not None:
                    options.append(f"{name.ljust(max_length)} ({d.get('description')})")
                else:
                    if d.get("children", {}) is not None and len(d.get("children", {})) > 0:
                        options.append(f"{name} ")
                    else:
                        options.append(f"{name}")
            options.append(None)
            return options[state]
        except Exception as e:
            print(f"{e=}")
            traceback.print_exception(*sys.exc_info())

    def _traverse(self, tokens, tree):
        if tree is None:
            return []
        elif len(tree) == 0:
            return []
        elif len(tokens) == 0:
            return []

        if len(tokens) == 1:
            options = []
            secondary_options = []
            for command, specification in tree.items():
                if command.startswith(tokens[0]):
                    options.append((command, specification))

                description = specification.get("description")
                if description is not None and tokens[0] in description:
                    secondary_options.append((command, specification))
            if len(options) == 0:
                return secondary_options
            return options
            # return [(x, tree[x]) for x in tree if x.startswith(tokens[0]) or tokens[0] in tree[x].get("description", "")]
        else:
            if tokens[0] in tree.keys():
                return self._traverse(tokens[1:], tree[tokens[0]]["children"])
            else:
                return []

    def _build_logic(self):
        self.logic = self.get_completions()

    def get_completions(self, skip: Optional[List[str]] = None):
        if skip is None:
            skip = []
        logic = {}
        handlers = self.cli.get_handlers()
        for command, handler in handlers.items():
            if command in skip:
                continue
            try:
                cmd = command.split(" ")
                completion = handler.auto_complete_choices(cmd)
                for key, options in completion.items():
                    logic[key] = options
            except Exception as e:
                print(f"Could not load auto completion for {command}")
                print(f"{e=}")
                traceback.print_exception(*sys.exc_info())
        return logic
