from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    NullType,
    TimestampType,
)


class RelationsSchema:

    @staticmethod
    def get_schema():
        return StructType(
            [
                StructField("account_id", StringType()),
                StructField("party_id", StringType()),
                StructField(
                    "partyIdentifier",
                    StructType(
                        [
                            StructField("operation", StringType()),
                            StructField("newValue", StringType()),
                            StructField("oldValue", NullType()),
                        ]
                    ),
                ),
                StructField(
                    "partyRelationshipType",
                    StructType(
                        [
                            StructField("operation", StringType()),
                            StructField("newValue", StringType()),
                            StructField("oldValue", NullType()),
                        ]
                    ),
                ),
                StructField(
                    "partyRelationStartDateTime",
                    StructType(
                        [
                            StructField("operation", StringType()),
                            StructField("newValue", TimestampType()),
                            StructField("oldValue", NullType()),
                        ]
                    ),
                ),
            ]
        )
