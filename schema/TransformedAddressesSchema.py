from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    NullType,
    DateType,
)


class TransformedAddressesSchema:

    @staticmethod
    def get_schema():
        return StructType(
            [
                StructField("party_id", StringType()),
                StructField(
                    "partyAddress",
                    StructType(
                        [
                            StructField("operation", StringType()),
                            StructField(
                                "newValue",
                                StructType(
                                    [
                                        StructField("addressLine1", StringType()),
                                        StructField("addressLine2", StringType()),
                                        StructField("addressCity", StringType()),
                                        StructField("addressPostalCode", StringType()),
                                        StructField("addressCountry", StringType()),
                                        StructField("addressStartDate", DateType()),
                                    ]
                                ),
                            ),
                            StructField("oldValue", NullType()),
                        ]
                    ),
                ),
            ]
        )
