def sanitize_power_net(net):
    if "profiles" in net:
        del net["profiles"]
    return net
