from pyspark.sql import DataFrame
from extract.Extractor import Extractor
from lib.EntitiesConfig import EntitiesConfig


class CsvExtractor(Extractor):

    def extract(self, entity_config: EntitiesConfig) -> DataFrame:
        return (
            self.spark.read.format("csv")
            .option("header", "true")
            .schema(entity_config.schema)
            .load(entity_config.source_location)
        )
