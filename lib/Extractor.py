import abc
from pyspark.sql import DataFrame


class Extractor(metaclass=abc.ABCMeta):
    def __init__(self, spark):
        self.spark = spark

    @abc.abstractmethod
    def extract(self, entity_config) -> DataFrame:
        pass
