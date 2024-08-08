from pyspark.sql import DataFrame
from lib.Extractor import Extractor
from lib.EntitiesConfig import EntitiesConfig
from lib.MissingSourceLocationError import MissingSourceLocationError


class HiveExtractor(Extractor):

    def extract(self, entity_config: EntitiesConfig) -> DataFrame:
        if len(entity_config.source_location) == 0:
            raise MissingSourceLocationError("Source location is required in sbdl.conf")

        return self.spark.sql(f"SELECT * FROM {entity_config.source_location}")
