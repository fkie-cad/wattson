import abc


class CCXReport(abc.ABC):
    def __init__(self):
        pass

    @abc.abstractmethod
    def to_dict(self) -> dict:
        ...
