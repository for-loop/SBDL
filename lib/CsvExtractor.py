from pyspark.sql import DataFrame
from lib.Extractor import Extractor


class CsvExtractor(Extractor):

    def extract(self, source_location) -> DataFrame:
        return (
            self.spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(source_location)
        )
