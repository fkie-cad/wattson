import abc


class WattsonRemoteObject(abc.ABC):
    @abc.abstractmethod
    def synchronize(self, force: bool = False, block: bool = True):
        """
        Synchronize this object with its instance in the simulation
        @param force: Whether to force an update
        @param block: Whether to block during the update
        @return:
        """
        ...
