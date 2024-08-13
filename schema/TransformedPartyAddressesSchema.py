from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    NullType,
    TimestampType,
    DateType,
    ArrayType,
)


class TransformedPartyAddressesSchema:

    @staticmethod
    def get_schema():
        return StructType(
            [
                StructField("account_id", StringType()),
                StructField(
                    "partyRelations",
                    ArrayType(
                        StructType(
                            [
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
                                StructField(
                                    "partyAddress",
                                    StructType(
                                        [
                                            StructField("operation", StringType()),
                                            StructField(
                                                "newValue",
                                                StructType(
                                                    [
                                                        StructField(
                                                            "addressLine1", StringType()
                                                        ),
                                                        StructField(
                                                            "addressLine2", StringType()
                                                        ),
                                                        StructField(
                                                            "addressCity", StringType()
                                                        ),
                                                        StructField(
                                                            "addressPostalCode",
                                                            StringType(),
                                                        ),
                                                        StructField(
                                                            "addressCountry",
                                                            StringType(),
                                                        ),
                                                        StructField(
                                                            "addressStartDate",
                                                            DateType(),
                                                        ),
                                                    ]
                                                ),
                                            ),
                                            StructField("oldValue", NullType()),
                                        ]
                                    ),
                                ),
                            ]
                        )
                    ),
                ),
            ]
        )
