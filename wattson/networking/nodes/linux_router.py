import mininet.node


class LinuxRouter(mininet.node.Node):
    """
    Implementation of a router.
    """

    def config(self, **params):
        super().config(**params)
        self.cmd('sysctl net.ipv4.ip_forward=1')

    def terminate(self):
        self.cmd('sysctl net.ipv4.ip_forward=0')
        super().terminate()