from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    NullType,
    TimestampType,
    DateType,
)


class PartiesSchema:

    @staticmethod
    def get_schema():
        return StructType(
            [
                StructField("load_date", DateType()),
                StructField("account_id", StringType()),
                StructField("party_id", StringType()),
                StructField("relation_type", StringType()),
                StructField("relation_start_date", TimestampType()),
            ]
        )
