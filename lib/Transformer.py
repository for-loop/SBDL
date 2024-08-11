from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, lit, struct


class Transformer:

    def __init__(self, spark):
        self.__spark = spark

    def __add_insert(self, column, alias):
        return struct(
            lit("INSERT").alias("operation"),
            column.alias("newValue"),
            lit(None).alias("oldValue"),
        ).alias(alias)

    def transform(self, df: DataFrame) -> DataFrame:
        return df.select(
            "account_id",
            "party_id",
            self.__add_insert(col("party_id"), "partyIdentifier"),
            self.__add_insert(col("relation_type"), "partyRelationshipType"),
            self.__add_insert(col("relation_start_date"), "partyRelationStartDateTime"),
        )
