
def main():
    """
    Main function to run the AutorunnerManager.
    """
    from wautorunner.manager.autorunner_manager import AutorunnerManager
    from logging import basicConfig, INFO, DEBUG

    # Configure logging
    basicConfig(level=DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    # Create an instance of AutorunnerManager
    manager = AutorunnerManager()
    # Execute the scenario
    manager.execute()


if __name__ == '__main__':
    main()

