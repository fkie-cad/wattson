def main():
    import logging
    from wattson.util import get_logger
    from wattson.util.compat import fix_iptc
    fix_iptc()

    format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=format_string)
    logger = get_logger('Wattson', 'Wattson')

    import wattson.topology.topology_starter
    wattson.topology.topology_starter.main()


def standalone():
    import logging
    from wattson.util import get_logger

    format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=format_string)
    logger = get_logger('Wattson Standalone', 'Wattson Standalone')

    import wattson.topology.standalone
    wattson.topology.standalone.main()
    

if __name__ == '__main__':
    main()
