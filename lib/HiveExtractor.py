from pyspark.sql import DataFrame
from lib.Extractor import Extractor


class HiveExtractor(Extractor):

    def extract(self, source_location) -> DataFrame:
        return self.spark.sql(f"SELECT * FROM {source_location}")
