"""
This package implements the powergrid of the co-simulation. The CoordServer
implements the main functionality by coupling a power flow simulation based on
pandapower (in PowerSimulator) with a ZMQ server. The RTUs and the MTU use a
CoordClient to communicate with the powergrid. There are two major communication
patterns:
1. REQ (client) - REP (server): used for registering at the coordinator and
getting/setting values in the pandapower simulation
2. SUB (client) - PUB (server): used for sending "global events". For now, this
is only used to send a "start" command to the clients when all of them have
registered.
"""

from wattson.powergrid.common.constants import *
from wattson.powergrid.client.coordination_client import CoordinationClient



