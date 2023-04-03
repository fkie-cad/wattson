import pandapower


def configure_grid(grid: pandapower.pandapowerNet):
    std_trafo_name = "25 MVA 110/20 kV"
    for idx in grid.trafo.index:
        pandapower.change_std_type(grid, idx, std_trafo_name, "trafo")
        grid.trafo.at[idx, "tap_pos"] = grid.trafo.at[idx, "tap_neutral"]
    return grid


