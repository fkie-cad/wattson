import ipaddress
import time
from typing import List, Dict, Type, Union, Optional, Tuple

from wattson.cosimulation.control.interface.wattson_client import WattsonClient
from wattson.cosimulation.control.messages.wattson_notification import WattsonNotification
from wattson.cosimulation.control.messages.wattson_response import WattsonResponse
from wattson.cosimulation.exceptions import NetworkNodeNotFoundException, NetworkEntityNotFoundException, InterfaceNotFoundException, NetworkException
from wattson.cosimulation.exceptions.node_creation_failed_exception import NodeCreationFailedException
from wattson.cosimulation.simulators.network.components.interface.network_node import NetworkNode
from wattson.cosimulation.simulators.network.components.remote.remote_network_entity import RemoteNetworkEntity
from wattson.cosimulation.simulators.network.components.remote.remote_network_entity_factory import RemoteNetworkEntityFactory
from wattson.cosimulation.simulators.network.components.remote.remote_network_host import RemoteNetworkHost
from wattson.cosimulation.simulators.network.components.remote.remote_network_interface import RemoteNetworkInterface
from wattson.cosimulation.simulators.network.components.remote.remote_network_link import RemoteNetworkLink
from wattson.cosimulation.simulators.network.components.remote.remote_network_node import RemoteNetworkNode
from wattson.cosimulation.simulators.network.components.remote.remote_network_router import RemoteNetworkRouter
from wattson.cosimulation.simulators.network.components.remote.remote_network_switch import RemoteNetworkSwitch
from wattson.cosimulation.simulators.network.messages.wattson_network_notificaction_topics import WattsonNetworkNotificationTopic
from wattson.cosimulation.simulators.network.messages.wattson_network_query import WattsonNetworkQuery
from wattson.cosimulation.simulators.network.messages.wattson_network_query_type import WattsonNetworkQueryType


class RemoteNetworkEmulator:
    _instances: Dict[int, 'RemoteNetworkEmulator'] = dict()

    @staticmethod
    def get_instance(wattson_client: WattsonClient) -> 'RemoteNetworkEmulator':
        _wattson_client_id = id(wattson_client)
        if _wattson_client_id not in RemoteNetworkEmulator._instances:
            RemoteNetworkEmulator._instances[_wattson_client_id] = RemoteNetworkEmulator(wattson_client=wattson_client)
        return RemoteNetworkEmulator._instances[_wattson_client_id]

    """
    Interfaces with the running network emulator to allow topology changes and receiving remote instances
    of NetworkEntities.
    """
    def __init__(self, wattson_client: WattsonClient):
        self._wattson_client = wattson_client
        self._last_updates = {}
        self._update_interval_seconds = 10
        self._links: Dict[str, RemoteNetworkLink] = {}
        self._nodes: Dict[str, RemoteNetworkNode] = {}
        self._interfaces: Dict[str, RemoteNetworkInterface] = {}
        self.logger = self._wattson_client.logger.getChild("RemoteNetworkEmulator")
        self._wattson_client.subscribe(WattsonNetworkNotificationTopic.TOPOLOGY_CHANGED, self._reload_entities)

    def synchronize(self, force: bool = False):
        self._update_nodes(force=force)
        self._update_links(force=force)
        self._update_interfaces(force=force)

    def query(self, query: WattsonNetworkQuery) -> WattsonResponse:
        return self._wattson_client.query(query)

    """
    SET GETTERS
    """
    def get_entities(self) -> List[RemoteNetworkEntity]:
        # noinspection PyTypeChecker
        return self.get_nodes() + self.get_links() + self.get_interfaces()

    def get_nodes(self) -> List[RemoteNetworkNode]:
        self._update_nodes()
        return [node for node in self._nodes.values() if isinstance(node, RemoteNetworkNode)]

    def get_switches(self) -> List[RemoteNetworkSwitch]:
        return [switch for switch in self.get_nodes() if isinstance(switch, RemoteNetworkSwitch)]

    def get_hosts(self) -> List[RemoteNetworkHost]:
        return [host for host in self.get_nodes() if isinstance(host, RemoteNetworkHost)]

    def get_routers(self) -> List[RemoteNetworkRouter]:
        return [router for router in self.get_nodes() if isinstance(router, RemoteNetworkRouter)]

    def get_links(self) -> List[RemoteNetworkLink]:
        self._update_links()
        return [link for link in self._links.values()]

    def get_interfaces(self) -> List[RemoteNetworkInterface]:
        self._update_interfaces()
        return [interface for interface in self._interfaces.values()]

    """
    INDIVIDUAL GETTERS
    """
    def get_node(self, node: Union[str, RemoteNetworkNode]) -> RemoteNetworkNode:
        if isinstance(node, RemoteNetworkNode):
            return node
        if node in self._nodes:
            return self._nodes[node]
        self._update_nodes(force=True)
        if node in self._nodes:
            return self._nodes[node]
        raise NetworkNodeNotFoundException(f"Node {node} does not exist")

    def get_host(self, host: Union[str, RemoteNetworkHost]) -> RemoteNetworkHost:
        host = self.get_node(host)
        if not isinstance(host, RemoteNetworkHost):
            raise NetworkNodeNotFoundException(f"Host {host} does not exist")
        return host

    def get_router(self, router: Union[str, RemoteNetworkRouter]) -> RemoteNetworkRouter:
        router = self.get_node(router)
        if not isinstance(router, RemoteNetworkRouter):
            raise NetworkNodeNotFoundException(f"Router {router} does not exist")
        return router

    def get_switch(self, switch: Union[str, RemoteNetworkSwitch]) -> RemoteNetworkSwitch:
        switch = self.get_node(switch)
        if not isinstance(switch, RemoteNetworkSwitch):
            raise NetworkNodeNotFoundException(f"Switch {switch} does not exist")
        return switch

    def get_link(self, link: Union[str, RemoteNetworkLink]) -> RemoteNetworkLink:
        if isinstance(link, RemoteNetworkLink):
            return link
        if link in self._links:
            return self._links[link]
        self._update_links(force=True)
        if link in self._links:
            return self._links[link]
        raise NetworkEntityNotFoundException(f"Link {link} does not exist")

    def get_interface(self, node: Union[str, RemoteNetworkNode], interface_id) -> RemoteNetworkInterface:
        node = self.get_node(node)
        for interface in node.get_interfaces():
            if interface.entity_id == interface_id:
                return interface
        node.synchronize(force=True)
        for interface in node.get_interfaces():
            if interface.entity_id == interface_id:
                return interface
        raise InterfaceNotFoundException(f"Node {node.entity_id} has no interface {interface_id}")

    def get_interface_by_id(self, entity_id: str) -> RemoteNetworkInterface:
        for interface in self.get_interfaces():
            if interface.entity_id == entity_id:
                return interface
        raise InterfaceNotFoundException(f"No interface with ID {entity_id} could be found")

    def has_interface(self, node: Union[str, RemoteNetworkNode], interface_id: str) -> bool:
        try:
            self.get_interface(node, interface_id)
            return True
        except InterfaceNotFoundException:
            return False
        except NetworkNodeNotFoundException:
            return False

    def get_unused_ip(self, subnet: ipaddress.IPv4Network) -> ipaddress.IPv4Address:
        query = WattsonNetworkQuery(
            query_type=WattsonNetworkQueryType.GET_UNUSED_IP,
            query_data={
                "subnet": subnet
            }
        )
        response = self._wattson_client.query(query)
        if not response.is_successful():
            error = response.data.get("error")
            raise NetworkException(f"No unused ip address in subnet {repr(subnet)} found: {error=}")
        return response.data.get("ip_address")

    """
    ENTITY ADDITION / CREATION
    """
    def create_node(self, entity_id: str, node_class: Type[NetworkNode] = RemoteNetworkNode,
                    arguments: Optional[dict] = None, config: Optional[dict] = None) -> RemoteNetworkNode:
        """
        Creates a new NetworkNode in the running network emulation.
        @param entity_id: The entity_id of the new node
        @param node_class: The node type class, i.e., NetworkNode or a respective subclass
        @param arguments: Arguments to pass to the node constructor
        @param config: The config to pass to the node constructor
        @return: A remote representation of the newly created node.
        """
        query = WattsonNetworkQuery(
            query_type=WattsonNetworkQueryType.ADD_NODE,
            query_data={
                "node_type": node_class.__name__.replace("Remote", "").replace("Wattson", ""),
                "entity_id": entity_id,
                "arguments": arguments if arguments is not None else {},
                "config": config if config is not None else {}
            }
        )
        response = self._wattson_client.query(query)
        if not response.is_successful():
            error = response.data.get("error", "")
            raise NodeCreationFailedException(f"Could not create {node_class.__name__} with {entity_id=}: {error=}")
        node = self.get_node(node=entity_id)
        return node

    def create_switch(self, entity_id: str, arguments: Optional[dict] = None, config: Optional[dict] = None) -> RemoteNetworkSwitch:
        switch = self.create_node(entity_id=entity_id, node_class=RemoteNetworkSwitch, arguments=arguments, config=config)
        if not isinstance(switch, RemoteNetworkSwitch):
            raise NodeCreationFailedException(f"Could not create switch with {entity_id=} - invalid node type returned")
        return switch

    def create_host(self, entity_id: str, arguments: Optional[dict] = None, config: Optional[dict] = None) -> RemoteNetworkHost:
        host = self.create_node(entity_id=entity_id, node_class=RemoteNetworkHost, arguments=arguments, config=config)
        if not isinstance(host, RemoteNetworkHost):
            raise NodeCreationFailedException(f"Could not create host with {entity_id=} - invalid node type returned")
        return host

    def create_router(self, entity_id: str, arguments: Optional[dict] = None, config: Optional[dict] = None) -> RemoteNetworkRouter:
        router = self.create_node(entity_id=entity_id, node_class=RemoteNetworkRouter, arguments=arguments, config=config)
        if not isinstance(router, RemoteNetworkRouter):
            raise NodeCreationFailedException(f"Could not create router with {entity_id=} - invalid node type returned")
        return router

    def create_interface(self, node_entity_id: str, arguments: Optional[dict] = None, config: Optional[dict] = None) -> RemoteNetworkInterface:
        """
        Creates a network interface at a given node and returns its remote representation.
        @param node_entity_id: The entity_id of the node to add the interface to
        @param arguments: Arguments to pass to the constructor
        @param config: Config options to pass to the constructor
        @return: The RemoteNetworkInterface instance
        """
        raise NotImplementedError("NIY")

    def remove_node(self, node: Union[str, RemoteNetworkNode]) -> bool:
        try:
            node = self.get_node(node)
        except NetworkNodeNotFoundException:
            self.logger.error("Invalid node - cannot be removed")
            return False
        query = WattsonNetworkQuery(
            query_type=WattsonNetworkQueryType.REMOVE_NODE,
            query_data={"entity_id": node.entity_id}
        )
        response = self._wattson_client.query(query)
        if not response.is_successful():
            error = response.data.get("error", "")
            self.logger.error(f"Could not remove node {node.entity_id}: {error=}")
            return False
        return True

    def remove_link(self, link: Union[str, RemoteNetworkLink]) -> bool:
        try:
            link = self.get_link(link)
        except NetworkEntityNotFoundException:
            self.logger.error("Invalid link - cannot be removed")
            return False
        query = WattsonNetworkQuery(
            query_type=WattsonNetworkQueryType.REMOVE_LINK,
            query_data={"entity_id": link.entity_id}
        )
        response = self._wattson_client.query(query)
        if not response.is_successful():
            error = response.data.get("error", "")
            self.logger.error(f"Could not remove link {link.entity_id}: {error=}")
            return False
        return True

    def connect_interfaces(self, interface_a: RemoteNetworkInterface, interface_b: RemoteNetworkInterface,
                           link_options: Optional[dict] = None) -> RemoteNetworkLink:
        raise NotImplementedError("NIY")

    def connect_nodes(self, node_a: Union[str, RemoteNetworkNode], node_b: Union[str, RemoteNetworkNode],
                      interface_a_options: Optional[dict] = None,
                      interface_b_options: Optional[dict] = None,
                      link_options: Optional[dict] = None,
                      update_default_routes: bool = False) -> Tuple[RemoteNetworkInterface, RemoteNetworkLink, RemoteNetworkInterface]:
        """
        Connects two nodes with a new link connecting two new interfaces.
        @param node_a: entity_id or RemoteNetworkNode instance of the first node.
        @param node_b: entity_id or RemoteNetworkNode instance of the second node.
        @param interface_a_options: Options for the newly created interface of node a.
        @param interface_b_options: Options for the newly created interface of node b.
        @param link_options: Options for the newly created lincd Doc    k.
        @param update_default_routes: Whether to update the default routes of the freshly connected nodes.
        @return: The newly created RemoteNetworkInterface at node a, the newly created RemoteNetworkLink and the newly created RemoteNetworkInterface at node b.
        """
        node_a = self.get_node(node_a)
        node_b = self.get_node(node_b)
        query = WattsonNetworkQuery(
            query_type=WattsonNetworkQueryType.CONNECT_NODES,
            query_data={
                "entity_id_a": node_a.entity_id,
                "entity_id_b": node_b.entity_id,
                "interface_a_options": interface_a_options,
                "interface_b_options": interface_b_options,
                "link_options": link_options,
                "update_default_routes": update_default_routes
            }
        )
        response = self._wattson_client.query(query)
        if not response.is_successful():
            error = response.data.get("error", "")
            raise NetworkException(f"Failed to connect nodes {error=}")
        interface_a = self.get_interface(node_a, response.data.get("interface_a"))
        interface_b = self.get_interface(node_b, response.data.get("interface_b"))
        link = self.get_link(response.data.get("link"))
        return interface_a, link, interface_b

    """
    INTERNAL SYNCHRONIZATION METHODS
    """
    def _reload_entities(self, notification: WattsonNotification):
        self.synchronize(force=True)

    def _update_nodes(self, force: bool = False):
        self._update_remote_objects(
            query_type=WattsonNetworkQueryType.GET_NODES,
            response_key="nodes",
            instance_class=RemoteNetworkNode,
            target=self._nodes,
            force=force
        )

    def _update_links(self, force: bool = False):
        self._update_remote_objects(
            query_type=WattsonNetworkQueryType.GET_LINKS,
            response_key="links",
            instance_class=RemoteNetworkLink,
            target=self._links,
            force=force
        )

    def _update_interfaces(self, force: bool = False):
        self._update_nodes(force=force)
        for node in self.get_nodes():
            for interface in node.get_interfaces():
                if interface.entity_id not in self._interfaces:
                    self._interfaces[interface.entity_id] = interface

    def _update_remote_objects(self,
                               query_type: WattsonNetworkQueryType,
                               response_key: str,
                               instance_class: Type[RemoteNetworkEntity],
                               target: Dict,
                               force: bool = False):
        if not force:
            if time.time() - self._last_updates.get(response_key, 0) < self._update_interval_seconds:
                return
        query = WattsonNetworkQuery(query_type=query_type)
        response = self._wattson_client.query(query=query)
        if not response.is_successful():
            error = response.data.get("error", "")
            self.logger.error(f"Could not load remote entities ({query_type.name}) {error=}")
            return
        deleted_entities = list(target.keys())
        for entity_id, entity_representation in response.data.get(response_key, {}).items():
            if entity_id in deleted_entities:
                deleted_entities.remove(entity_id)
            if entity_id in target:
                target[entity_id].update_from_remote_representation(entity_representation)
            else:
                entity = RemoteNetworkEntityFactory.get_remote_network_entity(self._wattson_client, remote_data_dict=entity_representation)
                if isinstance(entity, instance_class):
                    target[entity_id] = entity
                else:
                    self.logger.error(f"Could not create {instance_class.__name__} for {entity_id=}")
        for entity_id in deleted_entities:
            target.pop(entity_id)
        self._last_updates[response_key] = time.time()
