import unidecode

from wattson.cosimulation.control.co_simulation_controller import CoSimulationController
from wattson.cosimulation.simulators.network.components.wattson_network_node import WattsonNetworkNode


def generate_ssh_user_and_password(
        co_simulation_controller: CoSimulationController,
        node: WattsonNetworkNode,
        default_user: str = "admin",
        default_password: str = "admin",
        password_prefix: str = "secure_",
        password_postfix: str = "",
):

    model_manager = co_simulation_controller.get_model_manager()
    facilities = model_manager.get_models("facility")
    facility = facilities.get(node.get_config().get("facility_id"))
    if facility is None:
        return default_user, default_password

    readable_name = facility.get_readable_name()
    ascii_readable_name = unidecode.unidecode(readable_name).lower().replace(" ", "-").replace("`", "")
    username = ascii_readable_name
    password = f"{password_prefix}{ascii_readable_name}{password_postfix}"

    return username, password
