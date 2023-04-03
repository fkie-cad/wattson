import readline
from typing import List, Optional


class CLICompleter:
    def __init__(self, importer, cli):
        self.cli = cli
        self.hosts = [h["id"] for h in importer.get_hosts()]
        self.logic = {}
        self.importer = importer

    def setup(self):
        self._build_logic()

    def complete(self, text, state):
        try:
            tokens = readline.get_line_buffer().split()
            if not tokens or readline.get_line_buffer()[-1] == ' ':
                tokens.append("")
            results = self._traverse(tokens, self.logic) + [None]
            return results[state]
        except Exception as e:
            print(e)

    def _traverse(self, tokens, tree):
        if tree is None:
            return []
        elif len(tokens) == 0:
            return []
        if len(tokens) == 1:
            return [x + ' ' for x in tree if x.startswith(tokens[0])]
        else:
            if tokens[0] in tree.keys():
                return self._traverse(tokens[1:], tree[tokens[0]])
            else:
                return []

    def _build_logic(self):
        self.logic = self.get_completions()

    def get_completions(self, skip: Optional[List[str]] = None):
        if skip is None:
            skip = []
        logic = {}
        handlers = self.cli.handlers
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
        return logic
