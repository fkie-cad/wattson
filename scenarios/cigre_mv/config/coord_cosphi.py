import math

from wattson.powergrid.server.coord_logic_interface import CoordinatorLogicInterface


class CoordCosPhi(CoordinatorLogicInterface):
    def __init__(self, coordinator, args):
        super().__init__(coordinator, args)
        # TODO: Read these values from the RTU / let the RTU push these values to the coordinator?
        self.cos_phi = {
            "sgen": [
                lambda x: 0.95,
                lambda x: 0.95,
                lambda x: 0.95,
                lambda x: 0.95,
                lambda x: 0.95,
                lambda x: 0.95
            ],
            "storage": []
        }

    def setup(self, net):
        # Set Reactive Power for both sgens
        for index, _ in enumerate(self.cos_phi["sgen"]):
            value = net["sgen"].at[index, "p_mw"]
            cos_phi = self.cos_phi["sgen"][index](value)
            q_mvar = self._calculate_qmvar(value, cos_phi)
            net["sgen"].at[index, "q_mvar"] = q_mvar
            self.coordinator.logger.info(f"Setting sgen.{index} qmvar to {q_mvar}")

        # Set reactive power for storage 0
        for index, _ in enumerate(self.cos_phi["storage"]):
            value = net["storage"].at[index, "p_mw"]
            cos_phi = self.cos_phi["storage"][index](value)
            q_mvar = self._calculate_qmvar(value, cos_phi)
            net["storage"].at[index, "q_mvar"] = q_mvar
            self.coordinator.logger.info(f"Setting storage.0 qmvar to {q_mvar}")

    def write_transform(self, net, table, index, column, value) -> bool:
        if table in ["storage", "sgen"] and column == "p_mw":
            self.coordinator.logger.info(f"Running Write_Transform for: {table}.{index}.{column} = {value}")
            if table in self.cos_phi and index < len(self.cos_phi[table]):
                cos_phi = self.cos_phi[table][index](value)
                q_mvar = self._calculate_qmvar(value, cos_phi)
                self.coordinator.logger.info(f"Setting {table}.{index} qmvar to {q_mvar}")
                net[table].at[index, "q_mvar"] = q_mvar
            return False
        elif table == "trafo":
            return False
        return False

    def _calculate_qmvar(self, p_mw, cos_phi):
        s_mva = p_mw / cos_phi
        q_mvar = math.sqrt(s_mva ** 2 - p_mw ** 2)
        return q_mvar
