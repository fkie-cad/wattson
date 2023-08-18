from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from wattson.cosimulation.simulators.network.components.remote.remote_network_entity import RemoteNetworkEntity
    from wattson.cosimulation.simulators.network.components.remote.remote_network_host import RemoteNetworkHost
    from wattson.cosimulation.simulators.network.components.remote.remote_network_interface import RemoteNetworkInterface
    from wattson.cosimulation.simulators.network.components.remote.remote_network_link import RemoteNetworkLink
    from wattson.cosimulation.simulators.network.components.remote.remote_network_node import RemoteNetworkNode
    from wattson.cosimulation.simulators.network.components.remote.remote_network_router import RemoteNetworkRouter
    from wattson.cosimulation.simulators.network.components.remote.remote_network_switch import RemoteNetworkSwitch
    from wattson.cosimulation.simulators.network.components.remote.remote_network_docker_host import RemoteNetworkDockerHost

    from wattson.cosimulation.control.interface.wattson_client import WattsonClient
    from wattson.cosimulation.simulators.network.components.remote.remote_network_entity_representation import RemoteNetworkEntityRepresentation
    from wattson.cosimulation.simulators.network.components.remote.invalid_remote_network_entity import InvalidRemoteNetworkEntity


class RemoteNetworkEntityFactory:
    @staticmethod
    def get_remote_network_entity(wattson_client: 'WattsonClient', remote_data_dict: 'RemoteNetworkEntityRepresentation') -> 'RemoteNetworkEntity':
        entity_class = remote_data_dict.get("class")
        entity_id = remote_data_dict.get("entity_id")
        remote_data_dict.resolve(wattson_client=wattson_client)

        from wattson.cosimulation.simulators.network.components.remote.remote_network_host import RemoteNetworkHost
        from wattson.cosimulation.simulators.network.components.remote.remote_network_interface import RemoteNetworkInterface
        from wattson.cosimulation.simulators.network.components.remote.remote_network_link import RemoteNetworkLink
        from wattson.cosimulation.simulators.network.components.remote.remote_network_node import RemoteNetworkNode
        from wattson.cosimulation.simulators.network.components.remote.remote_network_router import RemoteNetworkRouter
        from wattson.cosimulation.simulators.network.components.remote.remote_network_switch import RemoteNetworkSwitch
        from wattson.cosimulation.simulators.network.components.remote.remote_network_docker_host import RemoteNetworkDockerHost
        from wattson.cosimulation.simulators.network.components.remote.remote_network_nat import RemoteNetworkNAT

        remote_class = {
            "WattsonNetworkNode": RemoteNetworkNode,
            "WattsonNetworkRouter": RemoteNetworkRouter,
            "WattsonNetworkDockerRouter": RemoteNetworkRouter,
            "WattsonNetworkHost": RemoteNetworkHost,
            "WattsonNetworkDockerHost": RemoteNetworkDockerHost,
            "WattsonNetworkNAT": RemoteNetworkNAT,
            "WattsonNetworkSwitch": RemoteNetworkSwitch,
            "WattsonNetworkLink": RemoteNetworkLink,
            "WattsonNetworkInterface": RemoteNetworkInterface
        }.get(entity_class)

        if remote_class is None:
            raise ValueError(f"No matching RemoteNetworkEntity found for {entity_id=} {entity_class=}")
        entity = remote_class(entity_id=entity_id, wattson_client=wattson_client, auto_sync=False)
        entity.update_from_remote_representation(remote_data_dict)
        return entity

    @staticmethod
    def get_invalid_entity(wattson_client: 'WattsonClient', entity_id: str) -> 'InvalidRemoteNetworkEntity':
        from wattson.cosimulation.simulators.network.components.remote.invalid_remote_network_entity import InvalidRemoteNetworkEntity
        return InvalidRemoteNetworkEntity(wattson_client=wattson_client, entity_id=entity_id)
