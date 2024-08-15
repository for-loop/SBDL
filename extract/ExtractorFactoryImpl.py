from extract.ExtractorFactory import ExtractorFactory
from extract.Extractor import Extractor
from extract.CsvExtractor import CsvExtractor
from extract.HiveExtractor import HiveExtractor


class ExtractorFactoryImpl(ExtractorFactory):

    def make_extractor(self, spark) -> Extractor:
        if self.conf["enable.hive"] == "true":
            return HiveExtractor(spark)

        return CsvExtractor(spark)
