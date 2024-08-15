import abc
from extract.Extractor import Extractor


class ExtractorFactory(metaclass=abc.ABCMeta):
    def __init__(self, conf):
        self.conf = conf

    @abc.abstractmethod
    def make_extractor(self, spark) -> Extractor:
        pass
