import datetime
import json
import time
from pathlib import Path

import pandapower as pp
import yaml

from wattson.powergrid.profiles.profile_loader_factory import ProfileLoaderFactory

here = Path(__file__).parent
power_net_yml = here.parent.parent.parent.joinpath("scenarios/simbench/mv-semiurb/powernetwork.yml")

with power_net_yml.open("r") as f:
    dicts = yaml.load(f, yaml.SafeLoader)

power_net = pp.from_json_string(json.dumps(dicts))

profile_provider_factory = ProfileLoaderFactory(power_net,
                                                interpolate="linear",
                                                profiles={"load": "simbench", "sgen": "simbench"},
                                                base_dir=power_net_yml.parent.joinpath("1-MV-semiurb--0-sw"))
profile_provider = profile_provider_factory.get_interface()
#profile_provider = PowerProfileProviderInterface(power_net,
#                                                 interpolate="linear",
#                                                 profiles={"load": "simbench", "sgen": "simbench"},
#                                                 base_dir=power_net_yml.parent.joinpath("1-MV-semiurb--0-sw"))
"""
power_net_2 = copy.deepcopy(power_net)
power_net_2.load.drop(columns="profile", inplace=True)
profile_provider_2 = PowerProfileProvider(power_net_2, profiles={"load": "default"})
"""
# value_load_5 = profile_provider.get_value("load", 5, datetime.datetime(year=2022, month=3, day=31, hour=11, minute=53))

# print(json.dumps(profile_provider_2._profiles, indent=4))

#date = datetime.datetime.strptime("2022-03-31 14:24:35", "%Y-%m-%d %H:%M:%S")
date = datetime.datetime.now()

print("")
print(date)
#print(power_net.load.iloc[5])
#print(power_net_2.load.iloc[5])
print("")
print("Value from Simbench (MW):")
try:
    while True:
        date = datetime.datetime.now() - datetime.timedelta(hours=1)
        print(date)
        print(profile_provider.get_value("sgen", 35, date))
        print("")
        time.sleep(5)
except KeyboardInterrupt:
    pass
#print("Value from Default (MW):")
#print(profile_provider_2.get_value("load", 5, date))

