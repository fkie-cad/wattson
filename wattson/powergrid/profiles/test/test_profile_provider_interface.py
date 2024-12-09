import json
from pathlib import Path

import pandapower
import yaml

from wattson.powergrid.profiles.profile_calculator import (
    ProfileCalculator,
)
import pytest


@pytest.mark.parametrize(
    "key, value, expected_filename",
    [
        ("test", "default", "test.json"),
        ("load", "simbench", "LoadProfile.csv"),
        ("sgen", "simbench", "RESProfile.csv"),
        ("test", "simbench", "test.csv"),
        ("test1", "test2", "test2"),
    ],
)

def test_store_base_values():
    scenario_path = (
        Path(__file__).parent.parent.parent.parent.with_name("scenarios")
        / "cigre_mv_2020"
    )
    dicts = yaml.load(
        (scenario_path / "powernetwork.yml").open("r"), Loader=yaml.FullLoader
    )
    pnet = pandapower.from_json_string(json.dumps(dicts))

    interface = ProfileCalculator(pnet, {})
    assert interface._base_values == {
        "load": {
            0: {"p": 15.3, "q": 3.044661557546264},
            1: {"p": 5.1, "q": 2.686591706977448},
            2: {"p": 0.285, "q": 0.069284900952516},
            3: {"p": 0.265, "q": 0.139597412225299},
            4: {"p": 0.445, "q": 0.108181687452175},
            5: {"p": 0.75, "q": 0.182328686717148},
            6: {"p": 0.565, "q": 0.137354277326918},
            7: {"p": 0.09, "q": 0.047410441887837},
            8: {"p": 0.5, "q": 0.099498743710662},
            9: {"p": 0.605, "q": 0.147078473951833},
            10: {"p": 0.675, "q": 0.35557831415878},
            11: {"p": 0.49, "q": 0.119121408655203},
            12: {"p": 0.08, "q": 0.042142615011411},
            13: {"p": 0.34, "q": 0.082655671311774},
            14: {"p": 0.5, "q": 0.099498743710662},
            15: {"p": 15.3, "q": 3.044661557546264},
            16: {"p": 5.28, "q": 1.648679471577178},
            17: {"p": 0.04, "q": 0.021071307505705},
            18: {"p": 0.215, "q": 0.052267556858916},
            19: {"p": 0.39, "q": 0.205445248180628},
        },
        "sgen": {
            0: {"p": 2.0, "q": 0.3},
            1: {"p": 2.0, "q": 0.3},
            2: {"p": 2.0, "q": 0.3},
            3: {"p": 2.0, "q": 0.3},
            4: {"p": 2.0, "q": 0.3},
            5: {"p": 2.0, "q": 0.3},
        },
    }
