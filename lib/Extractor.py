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


class Factory:

    def __init__(self, conf):
        self.conf = conf

    def make_extractor(self, spark):
        if self.conf["enable.hive"] == "true":
            return HiveExtractor(spark)

        return CsvExtractor(spark)
