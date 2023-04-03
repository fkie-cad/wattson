"""
This package contains data classes that represent this co-simulation's object
model on the SCADA environment. Hence, the classes contain all information
that is required for a certain entity. RTUs and MTUs can then be created from
instances of the corresponding classes.
The classes are equivalents of classes in CPS-IDS.
"""

from .devices import Device, HostDevice, MTUDevice, RTUDevice, \
    RouterDevice, SCADADevice, SwitchDevice
