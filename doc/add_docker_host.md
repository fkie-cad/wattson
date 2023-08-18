# Adding a Docker Host

## Introduction

This document aims to provide a step-by-step guide to extend a Wattson scenario by adding a docker host.

## Python Docker Host Extension

An extension can easily be done via python.
Below is an example for adding a new DockerHost.
Replace the name for the docker image you want to use. 
For example one from the wattson-docker repo.
Insert the ip of the switch you want to connect your host to.
Check the network.yml of the scenario to find a switch.
Note the "n" before the switch_id. 
It's important.
You can connect your host to a mirror port by uncommenting the respective line.

```python
class ExampleExtension(ScenarioExtension):
    def provides_pre_physical(self) -> bool:
        return True

    def extend_pre_physical(self):
        network_emulator = self.co_simulation_controller.network_emulator
        logger = self.co_simulation_controller.logger.getChild("ExampleExtension")
        # Configure the (Docker) Host with your image
        docker_config = {
            "id": "exampleid",
            "type": "docker-host",
            "image": "your-docker-image"
        }
        switch = network_emulator.get_switch("n<switch_id>")
        subnet = switch.get_subnets(include_management=False)[0]
        server_ip = network_emulator.get_unused_ip(subnet)
        # create host
        host = WattsonNetworkDockerHost(id=docker_config["id"], config=docker_config)
        network_emulator.add_host(host)
        # link host to switch
        link_model_server = NetworkLinkModel()
        link_model_server.delay_ms = 5
        network_emulator.connect_nodes(
            zeek_host, switch,
            interface_a_options={
                "ip_address": server_ip,
                "subnet_prefix_length": subnet.prefixlen
            },
            interface_b_options={
                #"config": {"mirror": True},
            },
            link_options={
                "link_model": link_model_server
            }
        )
```

For Wattson to load your extension it has to be added to the extensions.yml in the scenario folder.

```yaml
- file: extensions/example_extension.py
  class: ExampleExtension
  enabled: true
```

We can add services to our host by creating a "services" entry in the docker_config.

```python
docker_config["services"] = [{
    "module": "wattson.services.<service-file>",
    "service-type": "python",
    "class": "<service-class-name>",
    "autostart": False,
    "config": {"some-field": "some-value"}
}]
```

Once the simulation is started you can find the service id with:

```shell
Wattson> node info exampleid
```
And start the service with:

```shell
Wattson> service <id> start
```

### Example use case

We use a docker image that has installed all necessary packages for offering a ssh connection.
A WattsonService can then be used to start / stop the SSH daemon on the host.
See for reference the wattson-rtu-ssh example from the wattson-docker repository.