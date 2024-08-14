from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    NullType,
    TimestampType,
    DateType,
    ArrayType,
    IntegerType,
)


class TransformedSchema:

    @staticmethod
    def get_schema():
        return StructType(
            [
                StructField(
                    "eventHeader",
                    StructType(
                        [
                            StructField("eventIdentifier", StringType(), False),
                            StructField("eventType", StringType(), False),
                            StructField("majorSchemaVersion", IntegerType(), False),
                            StructField("minorSchemaVersion", IntegerType(), False),
                            StructField("eventDateTime", TimestampType(), False),
                        ]
                    ),
                    False,
                ),
                StructField(
                    "keys",
                    ArrayType(
                        StructType(
                            [
                                StructField("keyField", StringType(), False),
                                StructField("keyValue", StringType()),
                            ]
                        ),
                        False,
                    ),
                    False,
                ),
                StructField(
                    "payload",
                    StructType(
                        [
                            StructField(
                                "contractIdentifier",
                                StructType(
                                    [
                                        StructField("operation", StringType()),
                                        StructField("newValue", StringType()),
                                        StructField("oldValue", NullType()),
                                    ]
                                ),
                            ),
                            StructField(
                                "sourceSystemIdentifier",
                                StructType(
                                    [
                                        StructField("operation", StringType()),
                                        StructField("newValue", StringType()),
                                        StructField("oldValue", NullType()),
                                    ]
                                ),
                            ),
                            StructField(
                                "contactStartDateTime",
                                StructType(
                                    [
                                        StructField("operation", StringType()),
                                        StructField("newValue", TimestampType()),
                                        StructField("oldValue", NullType()),
                                    ]
                                ),
                            ),
                            StructField(
                                "contractTitle",
                                StructType(
                                    [
                                        StructField("operation", StringType()),
                                        StructField(
                                            "newValue",
                                            ArrayType(
                                                StructType(
                                                    [
                                                        StructField(
                                                            "contractTitleLineType",
                                                            StringType(),
                                                        ),
                                                        StructField(
                                                            "contractTitleLine",
                                                            StringType(),
                                                        ),
                                                    ]
                                                )
                                            ),
                                        ),
                                        StructField("oldValue", NullType()),
                                    ]
                                ),
                            ),
                            StructField(
                                "taxIdentifier",
                                StructType(
                                    [
                                        StructField("operation", StringType()),
                                        StructField(
                                            "newValue",
                                            StructType(
                                                [
                                                    StructField(
                                                        "taxIdType", StringType()
                                                    ),
                                                    StructField("taxId", StringType()),
                                                ]
                                            ),
                                        ),
                                        StructField("oldValue", NullType()),
                                    ]
                                ),
                            ),
                            StructField(
                                "contractBranchCode",
                                StructType(
                                    [
                                        StructField("operation", StringType()),
                                        StructField("newValue", StringType()),
                                        StructField("oldValue", NullType()),
                                    ]
                                ),
                            ),
                            StructField(
                                "contractCountry",
                                StructType(
                                    [
                                        StructField("operation", StringType()),
                                        StructField("newValue", StringType()),
                                        StructField("oldValue", NullType()),
                                    ]
                                ),
                            ),
                            StructField(
                                "partyRelations",
                                ArrayType(
                                    StructType(
                                        [
                                            StructField(
                                                "partyIdentifier",
                                                StructType(
                                                    [
                                                        StructField(
                                                            "operation", StringType()
                                                        ),
                                                        StructField(
                                                            "newValue", StringType()
                                                        ),
                                                        StructField(
                                                            "oldValue", NullType()
                                                        ),
                                                    ]
                                                ),
                                            ),
                                            StructField(
                                                "partyRelationshipType",
                                                StructType(
                                                    [
                                                        StructField(
                                                            "operation", StringType()
                                                        ),
                                                        StructField(
                                                            "newValue", StringType()
                                                        ),
                                                        StructField(
                                                            "oldValue", NullType()
                                                        ),
                                                    ]
                                                ),
                                            ),
                                            StructField(
                                                "partyRelationStartDateTime",
                                                StructType(
                                                    [
                                                        StructField(
                                                            "operation", StringType()
                                                        ),
                                                        StructField(
                                                            "newValue", TimestampType()
                                                        ),
                                                        StructField(
                                                            "oldValue", NullType()
                                                        ),
                                                    ]
                                                ),
                                            ),
                                            StructField(
                                                "partyAddress",
                                                StructType(
                                                    [
                                                        StructField(
                                                            "operation", StringType()
                                                        ),
                                                        StructField(
                                                            "newValue",
                                                            StructType(
                                                                [
                                                                    StructField(
                                                                        "addressLine1",
                                                                        StringType(),
                                                                    ),
                                                                    StructField(
                                                                        "addressLine2",
                                                                        StringType(),
                                                                    ),
                                                                    StructField(
                                                                        "addressCity",
                                                                        StringType(),
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
                                                        StructField(
                                                            "oldValue", NullType()
                                                        ),
                                                    ]
                                                ),
                                            ),
                                        ]
                                    )
                                ),
                            ),
                        ]
                    ),
                    False,
                ),
            ]
        )
