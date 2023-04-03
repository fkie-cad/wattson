"""
This script reads a pandapower net, continuously simulates it with a fixed UPS
and takes queries to update/retrieve values
"""
import datetime
from dateutil import tz
import time
from pathlib import Path
from typing import Union

import wattson
import pandapower
import pandapower.plotting
import pandapower.toolbox
import pandapower.auxiliary

from wattson.powergrid.messages import PPQuery
from wattson.powergrid.server.simulator_thread import SimulatorThread


class PowerSimulator:
    """
    An instance of this class performs the power flow simulation in the power
    grid. The work is done by the SimulatorThread.
    """

    def __init__(self, coordination_server, net: pandapower.pandapowerNet, ups: float, config: dict):
        self.coordinator: 'CoordinationServer' = coordination_server
        self.config = config
        pandapower.toolbox.clear_result_tables(net)
        
        net["converged"] = False
        self.logger = wattson.util.get_logger("PowerSimulator", "PowerSimulator")
        self.thread = SimulatorThread(self, net, ups, self.logger.getChild("SimulatorThread"), config=config)
        self.export_enable = self.config.get("export_enable", False)
        self.export_root = self.config.get("export_root", "powergrid-export")
        start_time = datetime.datetime.now().astimezone().strftime('%Y-%m-%d-%H-%M-%S')
        self.export_dir: Path = Path(self.export_root)
        self.export_pickle = True
        self.export_interval = self.config.get("export_interval", 1)
        self.export_filename_mode = self.config.get("export_name", "real_time")
        if self.export_enable:
            try:
                self.export_dir.mkdir(parents=True, exist_ok=True, mode=0o777)
                self.export_dir.chmod(0o755)
            except Exception as e:
                self.logger.error(f"Cannot create export dir: {e}")
                raise RuntimeError("Export cannot be initialized")

    def run(self):
        self.thread.start()

    # TODO sanitize query
    def add_update_query(self, query: PPQuery):
        if query.value is None:
            raise RuntimeError("Update must contain a non-None value")
        self.thread.add_update_query(query)
        self.logger.debug(f"Add update query: {query}")

    # TODO sanitize query
    def answer_retrieval_query(self, query: PPQuery):
        return self.thread.answer_retrieval_query(query)

    def get_powernet(self) -> pandapower.pandapowerNet:
        return self.thread.get_powernet()

    def stop(self):
        print("stopping power simulator")
        self.thread.stop()
        self.thread.join()
        print("Simulator thread terminated")

    def notify_element_updated(self, table, column, index, old_value, new_value):
        if new_value != old_value:
            self.coordinator.notify_element_updated(table, column, index, new_value)

    def notify_powerflow(self):
        if self.export_enable:
            # Export current grid
            start = time.time()
            try:
                if self.export_filename_mode == "real_time":
                    ref_time = datetime.datetime.utcnow()
                else:
                    ref_time = self.coordinator.get_current_simulated_time()
                filename = ref_time.strftime('%Y-%m-%d-%H-%M-%S-%f')[:-3] + ".grid"
                if self.export_pickle:
                    filename += ".p"
                    file = self.export_dir.joinpath(filename)
                    self.thread.net_to_pickle_file(file.absolute().__str__())
                    file.chmod(0o755)
                else:
                    file = self.export_dir.joinpath(filename)
                    grid = self.thread.net_to_yaml()
                    file.touch(mode=0o755)
                    with file.open("w") as f:
                        f.write(grid)
                self.logger.debug(f"Written grid in {(time.time() - start)} seconds to {filename}")
            except Exception as e:
                self.logger.error(f"Could not export grid: {e}")
        self.coordinator.notify_powerflow_completed()
