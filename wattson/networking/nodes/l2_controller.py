from wattson.networking.nodes.patched_ovs_controller import PatchedOVSController


class L2Controller(PatchedOVSController):
    def __init__(self, *args, **kwargs):
        # set wildcard such that matching only depends on L2
        kwargs['wildcard'] = "0x2820F0"
        super().__init__(*args, **kwargs)
