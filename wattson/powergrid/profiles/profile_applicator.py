import datetime

import pandapower
from wattson.powergrid.profiles.profile_provider_interface import PowerProfileProviderInterface


class ProfileApplicator:
    def __init__(self, power_grid: pandapower.pandapowerNet, provider: PowerProfileProviderInterface):
        self.net = power_grid
        self.provider = provider

    def apply(self, date_time: datetime.datetime):
        for element_type in ["load", "sgen"]:
            for element_id in self.net[element_type].index:
                for dimension in ["p", "q"]:
                    value = self.provider.get_value(element_type, element_id, date_time, dimension)
                    if value is None:
                        continue
                    col = {
                        "p": "p_mw",
                        "q": "q_mvar"
                    }.get(dimension)
                    self.net[element_type].at[element_id, col] = value
        return self.net

