import abc
from pyspark.sql import DataFrame
from conf.EntitiesConfig import EntitiesConfig


class Extractor(metaclass=abc.ABCMeta):
    def __init__(self, spark):
        self.spark = spark

    @abc.abstractmethod
    def extract(self, entity_config: EntitiesConfig) -> DataFrame:
        pass
