from lib.ExtractorFactory import ExtractorFactory
from lib.Extractor import Extractor
from lib.CsvExtractor import CsvExtractor
from lib.HiveExtractor import HiveExtractor


class ExtractorFactoryImpl(ExtractorFactory):

    def make_extractor(self, spark) -> Extractor:
        if self.conf["enable.hive"] == "true":
            return HiveExtractor(spark)

        return CsvExtractor(spark)
