# Communication and Power Network Specification


## File-Format
The configuration is based on Yaml.  
We represent both, the power and the communication network, in a single configuration.
Hereby, the power network specification does not require an underlying communication network awareness, i.e., the mapping of entities is initialized by the communication network components.
Per default, we utilize three different configuration files:
`powernetwork.yml`, `graph.yml`, and `datapoints.yml` to represent the power network,
the communication network and the mapping between both networks as well as the
semantics of IEC104 datapoints.

## Power Network
`powernetwork.yml`

The Power Network is represented by a YAML-representation of all DataFrames of
the original PandaPower Network.
Here, we use the index-based representation for each DataFrame.

## Communication Network
`graph.yml`
The Communication Network is represented by a YAML-representation of nodes and links.
Every Node is uniquely identifiable and has mandatory as well as optional attributes
resp. properties.

* IDs **never** contain dots, but only letters, numbers, and hyphens
* IDs *may* start with a digit. However, due to requirements of utilized software
  that requires a letter as the first symbol, hostnames are prefixed with an `n`
  in case *any* HostID starts with a digit.  

### Entities / Nodes
* Each Node requires a **unique** ID.
* Each Node has a list of interfaces with **unique** IDs within the Node.
  * For a Node `n1` and the interface `i1`, the global interface name is `n1.i1`
  * Each interface has a MAC address (*optional*)
  * Each interface can have an IP address (*optional*)
  * Each interface can be assigned a default gateway
    (Which is another node's interface, e.g. `n1.i2`) (*optional*)
* Nodes can have additional properties, such as `name`, `position2d`,
  `position3d`, `zone`, `owner`

#### Generic Host
* `type` = `host`
* `deploy` = `DeployObject`. See below
* `config` = Configuration Parameters for Deploy (*optional*)

MTUs and RTUs are basically Hosts.
However, the distinction allows to utilize default implementations and
configurations for MTUs and RTUs for simplicity.

#### Switch
* `type` = `switch`
* Interfaces only have an ID, but no IP-Address

#### Router
* `type` = `router`
* Every Interface has an IP-Address
* `routing-algorithm` = `[OSPF, ...]` (*optional*, Default: `OSPF`, NIY)

#### MTU
* `type` = `mtu`
* `rtu_ips` = `[IPv4Address]`. List of RTU IP-Addresses that this MTU connects to.
  Only required when the no deploy object is given. In this case, the `rtu_ips` are
  embedded into the standard MTU DeployObject.
* `deploy` = `DeployObject`. See below. *Optional*

#### RTU
* `type` = `rtu`
* `deploy` = `DeployObject`. See below. *Optional*

#### Attacker
* `type` = `attacker`
* `deploy` = `DeployObject`. See below

#### DeployObject
* `type` = [`python`, `docker`, `cmd`]
* `passconfig` = [`argument`, `tmpfile`] (How to pass the configuration object. Defaults to `tmpfile`)
* `config` = Configuration Object for Deploy (*optional*)

##### Python Deployment
This is a special case of Command-based Deployment.
It allows to instantiate Python objects from an arbitrary module that implement
the `FCS.deployment.PythonDeployment` interface.
The configuration is passed as a dictionary.

```
{
  type: python,
  module: FCS.deployment.myscenario
  class: MyRTU
  config: *ConfigurationObject*
}
```

##### Docker Deployment
TODO, NIY

##### Command-based Deployment
Allows to execute an arbitrary command on the Host.
The string `%config` placeholder is replaced by the escaped JSON representation
of the deployment configuration.

```
{
  type: cmd,
  cmd: 'php myfancyscript.php %config',
  config: *ConfigurationObject*
}
```

##### Configuration Object
This is basically a JSON Configuration that is passed to the deployed node.
However, the configuration object allows for references to nodes and datapoints.


#### Sample Network (4 Nodes)
For the corresponding links, see below.
```
                                              +---------- Node01 (n1)
                                              |
Node03 (n3)  ------ Router (router) ----- Switch (sw)
                                              |
                                              +---------- Node02 (n2)

```
* Router serves 2 subnets: 10.0.0.0/24 and 10.0.10.0/24
* Node03 is in subnet 10.0.10.0/24
* Node01 and Node02 are in subnet 10.0.0.0/24

These nodes are configured as follows:
```yaml
nodes:
  router:
    id: router
    name: Router
    type: router
    deploy:
      type: python    # python, docker, physical, none
      class: MyRouter # For python, give the class
    interfaces:
      - id: i1
        mac: 00:00:00:00:00:01
        ip: 10.0.0.1/24
      - id: i2
        mac: 00:00:00:00:00:02
        ip: 10.0.10.1/24
  sw:
    id: sw
    name: Switch
    type: switch
    interfaces:
      - id: i1
        mac: 00:00:00:00:01:01
      - id: i2
        mac: 00:00:00:00:01:02        
  n1:
    id: n1
    name: Node01
    type: Host
    deploy:
      type: python
      class: MyGenericNode
    interfaces:
      - id: i1
        mac: 00:00:00:00:02:01
        ip: 10.0.0.2/24
  n2:
    id: n2
    name: Node02
    type: Host
    deploy:
      type: python
      class: MyGenericNode
    interfaces:
      - id: i1
        mac: 00:00:00:00:03:01
        ip: 10.0.0.3/24
  n3:
    id: n3
    name: Node03
    type: Host
    deploy:
      type: docker
      # TODO
    interfaces:
      - id: i1
        mac: 00:00:00:00:04:01
        ip: 10.0.10.2/24
```

### Links
Links connect interfaces.
* Each Link has a unique ID.
* Each Link has a delay (*optional*, defaults to `5ms`)
* Optionally, a length can be given. In case no delay is given,
  the delay is calculated automatically (TODO: HOW?)
* Each Link has a maximum data-rate (*optional*, defaults to the maximum of `1Gbps`)
* Each Link has a packet loss rate (0-100) (*optional*, defaults to `0`)
* Each Link has a jitter specification (*optional*, defaults to `0ms`)
* Each Link connects to exactly two interfaces
* Futher optional attributes (with default values)
  * `red`:`bool` (`false`) - Random Early Discard (RED)
  * `ecn`:`bool` (`false`) - Explicit Congestion Notification (ECN)
  * `tbf`:`bool` (`false`) - TBF Scheduling (Token Bucket Filter)
  * `tbf_latency`:`str` (`None`) - TBF Latency Parameter
  * `hfsc`:`bool` (`false`) - HFSC Scheduling (Hierarchical fair-service curve)
  * `gro`:`bool` (`false`) - Enable GRO

```yaml
links:
  - id: l0
    delay: 10ms
    data-rate: 10Mbps
    interfaces:
      - router.i1
      - sw.i1
  - id: l1
    delay: 2ms
    data-rate: 100Mbps
    interfaces:
      - router.i2
      - n3.i1
  - id: l2
    delay: 2ms
    data-rate: 100Mbps
    interfaces:
      - sw.i1
      - n1.i1
  - id: l3
    delay: 2ms
    data-rate: 100Mbps
    interfaces:
      - sw.i2
      - n2.i1
```

## Datapoints
For a Wattson scenario, data point definitions are read from the `datapoints.yml`.


~~Datapoints specify the semantics of IEC104 messages as well as the mapping
between the power and the communication network.
For each node, a set of datapoints is given.
We specify them as Index-based YAML-representations of DataFrames for each Node,
whereby each data point has the following `13` attributes, whereby `ip` is
implicitly defined by the RTU.~~

[Read the data point format definition as of February 2021](DATAPOINTS.md).  

## Topology and Network Modifications
The configuration format allows for on-demand modifications of the topology
as well as the deployed communication network.
In particular, there are three different ways of modifications, which can be
combined arbitrarily.
All modifications are, however, always applied in the following order.

### Configuration-based Modification
Besides the integration of hosts and attackers in the YAML network configuration,
you can create the `extensions.yml` file that creates a list of additional
configuration files to apply, so called *Modifications*.
The paths are either absolute or relative to the `extensions.yml`.

```yaml
- /home/user/myattackscenario.yml  # absolute path
- pcap.yml                         # relative path to the extensions.yml
```

#### The *Modifications* File Format
Similar to the `graph.yml`, each Modification File can contain node and link
definitions. Additionally, the `config` key can be used to define Mininet related configurations. 

Nodes can either be created (by specifying unused IDs) or manipulated / overwritten.
Node Updates overwrite existing fields or extend lists and objects based
on the provided information.
In case a list should be overwritten and not extended, the first element of the
modified list has to be set to the magic string `__replace`.
The respective entry will be removed when applied to the existing node.
Similar, objects can be forced to overwrite the original by adding the magic property
`__replace: true`.
For instance, this allows to modify an existing interface of an existing node or extend
the list of interfaces such that a node has an additional interface.

Similarly, links can be created or replaced (not modified).
In both cases, a full link definition is required.
Links are replaced in case of a matching ID.

Different *Modifications* can overwrite each other.
The latest modification (latest list element in the `extensions.yml`) is applied last
and potentially overwrites changes by previous *Modifications*.

##### Config Modifications
File structure:

```yaml
config:
  key1: value1
  key2: value2
nodes:
  ...
links:
  ...
```

Available Configuration Keys:

* `switch`  
  Default: `"FCS.networking.nodes.patched_ovs_switch.PatchedOVSSwitch"`  
  The full Class Path for Switch Implementation to use
* `subnet_prefix_length`  
  Default: `24`  
  The IP Subnet Prefix Length
* `ip_base`  
  Default: `"0.0.0.0/0"`  
  The IP Base Network
* `link`  
  Default: `"mininet.link.TCLink"`  
  The full Class Path for the Link Implementation to use
* `controller`
  Default: `"FCS.networking.nodes.l2_controller.L2Controller"`  
  The full Class Path for the Layer 2 Controller to use (if applicable for selected switch)
* `use_v6`  
  Default: `False`  
  Whether to use IPv6 or not (IPv6 is not actively supported right now)

### Programmatic Modifications
In addition to configuration-based modifications, programmatic modifications enable
advanced and fine-granular modification possibilities.
Instead of relying on the expressiveness of the YAML-Configuration, both, the Mininet
Topology as well as the Mininet Network can be modified programmatically.

Those changes are applied **after** all configuration-based definitions have been applied.
Desired modifications are also specified in the `extensions.yml`.
However, instead of absolute or relative paths, each entry represents a Python
Object.

```yaml
- /home/user/myattackscenario.yml
- pcap.yml
- type: topology               # Modify the Topology
  module: my.python.module     # Module containing the Modificator
  class: MyTopologyModificator # Modificator Class (subclass of TopologyModificator)
- type: network
  module: my.python.module
  class: MyNetworkModificator  # Modificator Class (subclass of NetworkModificator)
  prestart: false              # Whether this Modification should be applied
                               # before or after the network has been started
- type: both
  module: my.python.module
  class: MyModificator         # Modificator Class
                               # (subclass of NetworkModificator and TopologyModificator)
  prestart: false              # Whether this Modification should be applied
                               # before or after the network has been started
```

#### Programmatic Topology Modification
Topology Modifications are applied after the configuration-based topology has been created,
but before the actual Mininet Network has been created.
The Modificator receives the topology, can apply arbitrary changes, and returns the modified
topology.

#### Programmatic Network Modification
Network Modifications are applied after the Mininet Network has been created based
on the topology.
The Modificator receives the network, can apply arbitrary changes, and returns the
potentially modified network.
Based on the `prestart` property, the changes are applied before or after the network
emulation has been started (defaults to `false`).

The type `both` allows to maintain an internal state between topology and network
modifications. In this case, the NetworkModificator is not instantiated as a new object,
but the instance created during the Topology Modification Phase is re-used.
