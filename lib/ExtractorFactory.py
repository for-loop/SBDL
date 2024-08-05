from lib.CsvExtractor import CsvExtractor
from lib.HiveExtractor import HiveExtractor


class ExtractorFactory:

    def __init__(self, conf):
        self.conf = conf

    def make_extractor(self, spark):
        if self.conf["enable.hive"] == "true":
            return HiveExtractor(spark)

        return CsvExtractor(spark)
