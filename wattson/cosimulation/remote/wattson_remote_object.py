import abc


class WattsonRemoteObject(abc.ABC):
    @abc.abstractmethod
    def synchronize(self, force: bool = False, block: bool = True):
        """
        Synchronize this object with its instance in the simulation

        Args:
            force (bool, optional):
                Whether to force an update
                (Default value = False)
            block (bool, optional):
                Whether to block during the update
                (Default value = True)
        """
        ...
