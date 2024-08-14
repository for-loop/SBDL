import abc

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import lit, struct


class Transformer(metaclass=abc.ABCMeta):

    def _add_insert(self, column, alias):
        return struct(
            lit("INSERT").alias("operation"),
            column.alias("newValue"),
            lit(None).alias("oldValue"),
        ).alias(alias)

    @abc.abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        pass
