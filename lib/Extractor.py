import abc
from pyspark.sql import DataFrame


class Extractor(metaclass=abc.ABCMeta):
    def __init__(self, spark):
        self.spark = spark

    @abc.abstractmethod
    def extract(self):
        pass


class HiveExtractor(Extractor):

    def extract(self, table) -> DataFrame:
        return self.spark.sql(f"SELECT * FROM {table}")


class CsvExtractor(Extractor):

    def extract(self, path) -> DataFrame:
        return (
            self.spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(path)
        )
