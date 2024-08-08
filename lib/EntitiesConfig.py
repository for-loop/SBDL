import abc


class EntitiesConfig(metaclass=abc.ABCMeta):

    def __init__(self, conf):
        self.conf = conf

    @property
    @abc.abstractmethod
    def source_location(self):
        pass

    @property
    @abc.abstractmethod
    def schema(self):
        pass
