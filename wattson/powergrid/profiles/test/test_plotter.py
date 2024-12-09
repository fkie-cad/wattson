from datetime import datetime, timedelta

from wattson.powergrid.profiles.plotter import ProfilePlotter
import pandapower.networks as pn


def main():
    profiles = {
        "load": "default"
    }
    power_grid = pn.create_cigre_network_mv(with_der="pv_wind")
    plotter = ProfilePlotter(power_grid=power_grid, profiles=profiles, noise="0", interpolate="steps")

    start_date = datetime.now()
    #start_date_str = "02.01.2022 15:00:00"
    #start_date = datetime.strptime(start_date_str, "%d.%m.%Y %H:%M:%S")
    #weights = plotter.provider._get_season_weights(start_date)
    #print(weights)
    end_date = start_date + timedelta(hours=24)
    #end_date = start_date + timedelta(days=365)
    plotter.plot(start_date, end_date, "load", 0, resolution=10000)


if __name__ == '__main__':
    main()
