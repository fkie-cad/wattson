import datetime
import json
from pathlib import Path

import pandapower as pp
import yaml

from wattson.powergrid.profiles.plotter import ProfilePlotter

here = Path(__file__).parent
power_net_yml = here.parent.parent.parent.joinpath("scenarios/simbench/mv-semiurb/powernetwork.yml")

with power_net_yml.open("r") as f:
    dicts = yaml.load(f, yaml.SafeLoader)

power_net = pp.from_json_string(json.dumps(dicts))

profiles = {"load": "simbench", "sgen": "simbench"}
plotter = ProfilePlotter(power_grid=power_net, profiles=profiles,
                         noise="0",
                         base_dir=power_net_yml.parent.joinpath("1-MV-semiurb--0-sw"),
                         interpolate="cubic")

start_date = datetime.datetime.strptime("2021-12-22 00:00:00", "%Y-%m-%d %H:%M:%S")
#start_date = datetime.datetime.now()
#end_date = start_date + datetime.timedelta(minutes=8*6)
end_date = start_date + datetime.timedelta(hours=24)

days = (end_date - start_date).days
measures_per_day = 24
values = days * measures_per_day

values = 24*12

element_collection = set()
loads = set()
sgens = set()
for load_id in power_net.load.index:
    e = ("load", int(load_id))
    loads.add(e)
for sgen_id in power_net.sgen.index:
    e = ("sgen", int(sgen_id))
    sgens.add(e)
loads = frozenset(loads)
sgens = frozenset(sgens)
element_collection.add(("Load (MW)", loads))
element_collection.add(("Generation (MW)", sgens))
plotter.plot(start_date=start_date, end_date=end_date, elements=element_collection, resolution=values)
