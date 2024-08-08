from pyspark.sql import DataFrame
from lib.Extractor import Extractor
from lib.MissingSchemaError import MissingSchemaError
from lib.EntitiesConfig import EntitiesConfig


class CsvExtractor(Extractor):

    def extract(self, entity_config: EntitiesConfig) -> DataFrame:
        if len(entity_config.schema) == 0:
            raise MissingSchemaError("Schema is required in sbdl.conf")

        return (
            self.spark.read.format("csv")
            .option("header", "true")
            .schema(entity_config.schema)
            .load(entity_config.source_location)
        )
