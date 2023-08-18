import re

import mininet.log
import mininet.node
import mininet.util
from packaging import version


class PatchedOVSController(mininet.node.OVSController):
    def __init__(self, *args, **kwargs):
        kwargs['command'] = 'ovs-testcontroller'
        super().__init__(*args, **kwargs)

        # add argument "wildcard"
        wc = kwargs.get("wildcard")
        if wc is not None:
            if wc == "":
                self.add_carg("-w")
            else:
                self.add_carg("--wildcards=\"" + wc + "\"")

        # check version
        version_str = mininet.util.quietRun( self.command + ' --version' )
        parsed_version = version.parse(re.findall( r'\d+\.\d+', version_str )[ 0 ])
        if parsed_version < version.parse("2"):
            mininet.log.error("This is not OVS version 2!")

        # set allowed openflow versions (1.5 not allowed since the controller
        # crashes for some reason)
        # check if it works for later versions of the controller..
        of_ver = "\"OpenFlow10 OpenFlow11 OpenFlow12 OpenFlow13 OpenFlow14\""
        self.add_carg("-O " + of_ver)

#         check if patched
        help_str = mininet.util.quietRun(self.command + " --help")
        if ', patched for ' not in help_str:
            mininet.log.warn("This seems to be an unpatched version! "
                             "Possibly only 16 switches are allowed!\n")

    def add_carg(self, arg):
        """
        Add a controller argument. Must be appended since the last argument
        must be the connect method and port.
        :param arg: argument
        :return:
        """
        self.cargs = " " + str(arg) + " " + self.cargs
