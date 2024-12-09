from datetime import datetime, timedelta
from typing import Tuple, Set, Union

import pandapower
from matplotlib import pyplot as plt

from .profile_loader_factory import ProfileLoaderFactory

PlotterElement = Tuple[str, int]
PlotterElementGroup = Tuple[str, Set[PlotterElement]]
PlotterElementSet = Set[PlotterElementGroup]
PlotterElementCollection = Union[PlotterElement, PlotterElementGroup, PlotterElementSet]




class ProfilePlotter:
    def __init__(self, power_grid: pandapower.pandapowerNet, profiles: dict,
                 seed: int = 0, noise: str = "1%", interpolate="cubic", **kwargs):
        self.power_grid = power_grid
        self.profile_provider_factory = ProfileLoaderFactory(power_grid, profiles, seed=seed, noise=noise, interpolate=interpolate,
                                                             **kwargs)
        self.provider = self.profile_provider_factory.get_interface()

        #self.provider = PowerProfileProviderInterface(simulator, profiles, seed=seed, noise=noise, interpolate=interpolate,
        #                                              **kwargs)

    def plot(self, start_date: datetime, end_date: datetime, elements: PlotterElementCollection, resolution=1000):
        if type(elements) == PlotterElement:
            group_name = f"{elements[0]}.{elements[1]}"
            element_tuple = (group_name, elements)
            elements = set(set(element_tuple))
        elif type(elements) == PlotterElementGroup:
            elements = set(elements)
        element_set = elements
        time_range = (end_date - start_date).total_seconds()
        resolution = max(resolution, 2)
        step_size = time_range / (resolution-1)
        step_date = start_date
        x = {}
        y = {}
        while step_date <= end_date:
            for group in element_set:
                g_name = group[0]
                elements = group[1]
                value = 0
                x.setdefault(g_name, []).append(step_date)
                for element in elements:
                    value += self.provider.get_value(
                        element_type=element[0],
                        element_index=element[1],
                        date_time=step_date
                    )
                y.setdefault(g_name, []).append(value)
            step_date = step_date + timedelta(seconds=step_size)
        fig, ax = plt.subplots(constrained_layout=True)
        for g_name in x.keys():
            ax.plot(x[g_name], y[g_name], ".-", label=g_name)
        ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
        plt.show()
