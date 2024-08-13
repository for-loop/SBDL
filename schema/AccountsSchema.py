from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DateType,
    IntegerType,
)


class AccountsSchema:

    @staticmethod
    def get_schema():
        return StructType(
            [
                StructField("load_date", DateType()),
                StructField("active_ind", IntegerType()),
                StructField("account_id", StringType()),
                StructField("source_sys", StringType()),
                StructField("account_start_date", TimestampType()),
                StructField("legal_title_1", StringType()),
                StructField("legal_title_2", StringType()),
                StructField("tax_id_type", StringType()),
                StructField("tax_id", StringType()),
                StructField("branch_code", StringType()),
                StructField("country", StringType()),
            ]
        )
