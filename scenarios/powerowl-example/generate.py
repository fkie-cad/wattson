from pathlib import Path

import numpy as np
from powerowl.derivators.default_derivator import DefaultDerivator
from powerowl.performance.timing import Timing
import pandapower.networks as ppn
from powerowl.power_owl import PowerOwl
from powerowl.simulators.pandapower import PandaPowerGridModel

Timing.enabled = True
Timing.sub_timing_visibility_level = 3

with Timing("Total"):
    with Timing("Power Grid Parsing"):
        net = ppn.create_cigre_network_mv(with_der="all")

        for idx in list(net.storage.index):
            max_p_mw = net.storage.at[idx, "max_p_mw"]
            max_e_mwh = net.storage.at[idx, "max_e_mwh"]
            if max_e_mwh is None or np.isnan(max_e_mwh):
                max_e_mwh = round(max_p_mw * 3)
            net.storage.at[idx, "max_e_mwh"] = max_e_mwh

        grid_model = PandaPowerGridModel()
        grid_model.from_external(net)
        # Import pandapower model into PowerOwl
        owl = PowerOwl(power_grid=grid_model)
        owl.derive(
            derivator_class=DefaultDerivator,
            config={
                "abstract-from-field-devices": True,
                "field-device-attachment": "rtu-hub",
                "network-segregation-degree": 1,
                "force-subnet-tree": True,
                "manageable-switches": False,
            }
        )

    with Timing("Visualization"):
        with Timing("Layout"):
            owl.layout()
    with Timing("Export"):
        owl.export_scenario(Path(__file__).parent)
