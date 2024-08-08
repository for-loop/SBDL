from pyspark.sql import DataFrame
from lib.Extractor import Extractor


class HiveExtractor(Extractor):

    def extract(self, entity_config) -> DataFrame:
        return self.spark.sql(f"SELECT * FROM {entity_config.source_location}")
