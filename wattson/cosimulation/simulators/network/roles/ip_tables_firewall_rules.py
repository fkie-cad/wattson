


class IPTablesFirewallRules(dict):
    def __init__(self, rule):
        super().__init__()
        if rule.startswith("iptables"):
            self.rule = rule
        else:
            self.rule = f"iptables {rule}"
        self.command = self.rule.split(" ")[0]
        self.chain = self.rule.split(" ")[1]
        self.specification = " ".join(rule.split(" ")[2:])

    @property
    def chain(self):
        return self.get("chain", "None")

    @property
    def specification(self):
        return self.get("specification", "None")

    @property
    def command(self):
        return self.get("command", "None")

    @property
    def rule(self):
        return self.get("rule", "None")

    @rule.setter
    def rule(self, value):
        self["rule"] = value

    @command.setter
    def command(self, value):
        self["command"] = value

    @specification.setter
    def specification(self, value):
        self["specification"] = value

    @chain.setter
    def chain(self, value):
        self["chain"] = value

    def __str__(self):
        return self["rule"]





