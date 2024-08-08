from pyspark.sql import DataFrame
from lib.Extractor import Extractor
from lib.EntitiesConfig import EntitiesConfig


class HiveExtractor(Extractor):

    def extract(self, entity_config: EntitiesConfig) -> DataFrame:
        return self.spark.sql(f"SELECT * FROM {entity_config.source_location}")
