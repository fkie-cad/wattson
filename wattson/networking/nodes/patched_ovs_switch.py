import mininet.log
import mininet.node
from mininet.link import TCIntf
from mininet.util import errRun
from packaging import version


class PatchedOVSSwitch(mininet.node.OVSSwitch):
    def __init__(self, *args, **kwargs):
        kwargs["batch"] = True
        self._started = False
        kwargs["failMode"] = "standalone"
        super().__init__(*args, **kwargs)

    def vsctl( self, *args, **kwargs ):
        "Run ovs-vsctl command (or queue for later execution)"
        if self.batch:
            cmd = ' '.join(str(arg).strip() for arg in args)
            if cmd[:2] != "--":
                cmd = "-- " + cmd
            self.commands.append(cmd)
            return None
        else:
            return self.cmd( 'ovs-vsctl', *args, **kwargs )

    def bridgeOpts(self):
        opts = super().bridgeOpts()
        if self.params.get("rstp", True) and self.failMode == 'standalone':
            opts += ' rstp_enable=true'
        return opts

    def attach( self, intf):
        if self._started:
            super(PatchedOVSSwitch, self).attach(intf)

    def start(self, controllers):
        "Start up a new OVS OpenFlow switch using ovs-vsctl"
        if self.inNamespace:
            raise Exception('OVS kernel switch does not work in a namespace')

        int(self.dpid, 16)  # DPID must be a hex string
        if self.shell is None:
            self.startShell()

        # Command to add interfaces
        intfs = [' -- add-port %s %s' % (self.deployed_name, intf) +
                        self.intfOpts(intf)
                        for intf in self.intfList()
                        if self.ports[intf] and not intf.IP()]

        # Command to create controller entries
        clist = [(self.deployed_name + c.name, '%s:%s:%d' %
                  (c.protocol, c.IP(), c.port))
                 for c in controllers]
        if self.listenPort:
            clist.append((self.deployed_name + '-listen', 'ptcp:%s' % self.listenPort))
        ccmd = '-- --id=@%s create Controller target=\\"%s\\"'

        if self.reconnectms:
            ccmd += ' max_backoff=%d' % self.reconnectms
        cargs = ' '.join(ccmd % (name, target) for name, target in clist)

        # Controller ID list
        cids = ','.join('@%s' % name for name, _target in clist)
        # Try to delete any existing bridges with the same name
        if not self.isOldOVS():
            cargs += ' -- --if-exists del-br %s' % self.deployed_name
        # One ovs-vsctl command to rule them all!
        self.vsctl(cargs +
                   ' -- add-br %s' % self.deployed_name +
                   ' -- set bridge %s controller=[%s]' % (self.deployed_name, cids) +
                   self.bridgeOpts())

        # Add Interfaces - make sure to not exceed command length limit
        while len(intfs) > 0:
            part = intfs[:20]
            intfs = intfs[20:]
            self.vsctl("".join(part))
        # If necessary, restore TC config overwritten by OVS
        if not self.batch:
            for intf in self.intfList():
                self.TCReapply(intf)
        self._started = True

    @classmethod
    def setup(cls):
        super().setup()
        if version.parse(cls.OVSVersion) < version.parse('2'):
            mininet.log.error("This is not OVS version 2!")

    @classmethod
    def batchStartup(cls, switches, run=errRun):
        """Batch startup for OVS
           switches: switches to start up
           run: function to run commands (errRun)"""
        mininet.log.info('...')
        cmds = 'ovs-vsctl'
        for switch in switches:
            if switch.isOldOVS():
                # Ideally we'd optimize this also
                run('ovs-vsctl del-br %s' % switch)
            for cmd in switch.commands:
                cmd = cmd.strip()
                # Don't exceed ARG_MAX
                if len(cmds) + len(cmd) >= cls.argmax:
                    run(cmds, shell=True)
                    cmds = 'ovs-vsctl'
                cmds += ' ' + cmd
                switch.cmds = []
                switch.batch = False
        if cmds:
            run(cmds, shell=True)
        # Reapply link config if necessary...
        return switches
        for switch in switches:
            for intf in switch.intfs.values():
                if isinstance(intf, TCIntf):
                    intf.config(**intf.params)
        return switches
