from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
)


class AddressesSchema:

    @staticmethod
    def get_schema():
        return StructType(
            [
                StructField("load_date", DateType()),
                StructField("party_id", StringType()),
                StructField("address_line_1", StringType()),
                StructField("address_line_2", StringType()),
                StructField("city", StringType()),
                StructField("postal_code", StringType()),
                StructField("country_of_address", StringType()),
                StructField("address_start_date", DateType()),
            ]
        )
