from mininet.link import TCIntf
from mininet.log import error, debug


class WattsonIPInterface(TCIntf):
    def __init__(self, name, node=None, port=None, link=None,
                 mac=None, **params):
        self.args = []
        self.default_route = None
        super().__init__(name, node, port, link, mac, **params)

    def isUp(self, setUp=False):
        if setUp:
            cmdOutput = self.ifconfig('up')
            # no output / command output indicates success
            if (len(cmdOutput) > 0
                    and "ifconfig" not in cmdOutput):
                from mininet.node import Docker
                if not isinstance(self.node, Docker):
                    error("Error setting %s up: %s " % (self.name, cmdOutput))
                return False
            else:
                return True
        else:
            return "UP" in self.ifconfig()

    def bwCmds(self, bw=None, speedup=0, use_hfsc: bool = False, use_tbf: bool = False,
               latency_ms=None, enable_ecn: bool = False, enable_red: bool = False,
               smooth_change: bool = False):

        cmds, parent = [], ' root '

        if bw and (bw < 0 or bw > self.bwParamMax):
            error(
                'Bandwidth limit', bw, 'is outside supported range 0..%d'
                % self.bwParamMax, '- ignoring\n'
                )
        elif bw is not None:
            smooth_change_str = 'change' if smooth_change else 'add'

            # BL: this seems a bit brittle...
            if (speedup > 0 and
                    self.node.name[0:1] == 's'):
                bw = speedup
            # This may not be correct - we should look more closely
            # at the semantics of burst (and cburst) to make sure we
            # are specifying the correct sizes. For now I have used
            # the same settings we had in the mininet-hifi code.
            if use_hfsc:
                if not smooth_change:
                    cmds += ['%s qdisc add dev %s root handle 5:0 hfsc default 1']
                cmds += ['%s class ' + smooth_change_str + ' dev %s parent 5:0 classid 5:1 hfsc sc '
                         + 'rate %fMbit ul rate %fMbit' % (bw, bw)]
            elif use_tbf:
                if latency_ms is None:
                    latency_ms = 15 * 8 / bw
                if smooth_change:
                    error("tbf does not support smooth change")

                cmds += ['%s qdisc add dev %s root handle 5: tbf ' +
                         'rate %fMbit burst 15000 latency %fms' %
                         (bw, latency_ms)]
            else:
                if not smooth_change:
                    cmds += ['%s qdisc add dev %s root handle 5:0 htb default 1']
                cmds += ['%s class ' + smooth_change_str +
                         ' dev %s parent 5:0 classid 5:1 htb ' +
                         'rate %fMbit burst 15k' % bw]
            parent = ' parent 5:1 '

            # ECN or RED
            if enable_ecn:
                cmds += ['%s qdisc add dev %s' + parent +
                         'handle 6: red limit 1000000 ' +
                         'min 30000 max 35000 avpkt 1500 ' +
                         'burst 20 ' +
                         'bandwidth %fmbit probability 1 ecn' % bw]
                parent = ' parent 6: '
            elif enable_red:
                cmds += ['%s qdisc add dev %s' + parent +
                         'handle 6: red limit 1000000 ' +
                         'min 30000 max 35000 avpkt 1500 ' +
                         'burst 20 ' +
                         'bandwidth %fmbit probability 1' % bw]
                parent = ' parent 6: '
        return cmds, parent

    @staticmethod
    def delayCmds(parent, delay=None, jitter=None,
                  loss=None, max_queue_size=None, smooth_change=False):
        """Internal method: return tc commands for delay and loss"""
        cmds = []
        if loss and ( loss < 0 or loss > 100 ):
            error('Bad loss percentage', loss, '%%\n' )
        else:
            # Delay/jitter/loss/max queue size
            netemargs = '%s%s%s%s' % (
                'delay %s ' % delay if delay is not None else '',
                '%s ' % jitter if jitter is not None else '',
                'loss %.5f ' % loss if loss is not None else '',
                'limit %d' % max_queue_size if max_queue_size is not None
                else ''
            )
            if netemargs:
                smooth_change_str = 'change' if smooth_change else 'add'
                cmds = ['%s qdisc ' + smooth_change_str + ' dev %s ' + parent +
                        ' handle 10: netem ' +
                        netemargs]
                parent = ' parent 10:1 '
        return cmds, parent

    def requiresHardReset(self, bw, delay, jitter, loss, max_queue_size, use_hfsc, use_tbf):
        # pessimistic assumptions: always require hard reset when limits are
        # set from not None to None or the tc type changes
        return ((bw is None and self.bw is not None) or
                (delay is None and self.delay is not None) or
                (jitter is None and self.jitter is not None) or
                (loss is None and self.loss is not None) or
                (max_queue_size is None and self.max_queue_size is not None) or
                self.use_hfsc is not use_hfsc or
                self.use_tbf is not use_tbf)

    def storeConfig(self, bw, delay, jitter, loss, max_queue_size, use_hfsc, use_tbf):
        self.bw = bw
        self.delay = delay
        self.jitter = jitter
        self.loss = loss
        self.max_queue_size = max_queue_size
        self.use_hfsc = use_hfsc
        self.use_tbf = use_tbf

    def firstTimeConfig(self):
        # any attribute could be used for this...
        return not hasattr(self, 'use_hfsc')

    def config(self, bw=None, delay=None, jitter=None, loss=None,
               disable_gro=True, speedup=0, use_hfsc=False, use_tbf=False,
               latency_ms=None, enable_ecn=False, enable_red=False,
               max_queue_size=None, smooth_change=False, **params):
        """Configure the port and set its properties."""

        result = super().config(**params)
        if result is None:
            result = {}

        # Disable GRO
        if disable_gro:
            self.cmd('ethtool -K %s gro off' % self)

        # Optimization: return if nothing else to configure and nothing changed.
        # Note that the attribute 'use_hfsc' is only available if previous calls
        # passed this check.
        if (bw is None and delay is None and loss is None
                and max_queue_size is None and self.firstTimeConfig()):
            return

        if smooth_change and self.firstTimeConfig():
            error("smooth change is not support for setting initial values")
            smooth_change = False

        if (smooth_change and
                self.requiresHardReset(bw, delay, jitter, loss, max_queue_size, use_hfsc, use_tbf)):
            error("smooth change is not support if tc type changes or limits set to None")
            smooth_change = False

        self.storeConfig(bw, delay, jitter, loss, max_queue_size, use_hfsc, use_tbf)

        # Clear existing configuration
        tcoutput = self.tc('%s qdisc show dev %s')
        if ("priomap" not in tcoutput and "noqueue" not in tcoutput
                and not smooth_change):
            cmds = ['%s qdisc del dev %s root']
        else:
            cmds = []

        # Bandwidth limits via various methods
        bwcmds, parent = self.bwCmds(
            bw=bw, speedup=speedup,
            use_hfsc=use_hfsc, use_tbf=use_tbf,
            latency_ms=latency_ms,
            enable_ecn=enable_ecn,
            enable_red=enable_red,
            smooth_change=smooth_change
            )
        cmds += bwcmds

        # Delay/jitter/loss/max_queue_size using netem
        delaycmds, parent = self.delayCmds(
            delay=delay, jitter=jitter,
            loss=loss,
            max_queue_size=max_queue_size,
            parent=parent,
            smooth_change=smooth_change
            )
        cmds += delaycmds

        # Ugly but functional: display configuration info
        stuff = ((['%.2fMbit' % bw] if bw is not None else []) +
                 (['%s delay' % delay] if delay is not None else []) +
                 (['%s jitter' % jitter] if jitter is not None else []) +
                 (['%.5f%% loss' % loss] if loss is not None else []) +
                 (['ECN'] if enable_ecn else ['RED']
                 if enable_red else []))
        debug('(' + ' '.join(stuff) + ') ')

        # Execute all the commands in our node
        debug("at map stage w/cmds: %s\n" % cmds)
        tcoutputs = [self.tc(cmd) for cmd in cmds]
        for output in tcoutputs:
            if output != '':
                error("*** Error: %s" % output)
        debug("cmds:", cmds, '\n')
        debug("outputs:", tcoutputs, '\n')
        result['tcoutputs'] = tcoutputs
        result['parent'] = parent

        return result

    def cmd(self, *args, **kwargs):
        return self.node.cmd(*args, **kwargs)

    def __repr__(self):
        return super().__repr__()

    def __str__(self):
        return self.name
