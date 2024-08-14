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
                                        StructField("operation", StringType(), False),
                                        StructField("newValue", StringType()),
                                        StructField("oldValue", NullType()),
                                    ]
                                ),
                                False,
                            ),
                            StructField(
                                "sourceSystemIdentifier",
                                StructType(
                                    [
                                        StructField("operation", StringType(), False),
                                        StructField("newValue", StringType()),
                                        StructField("oldValue", NullType()),
                                    ]
                                ),
                                False,
                            ),
                            StructField(
                                "contactStartDateTime",
                                StructType(
                                    [
                                        StructField("operation", StringType(), False),
                                        StructField("newValue", TimestampType()),
                                        StructField("oldValue", NullType()),
                                    ]
                                ),
                                False,
                            ),
                            StructField(
                                "contractTitle",
                                StructType(
                                    [
                                        StructField("operation", StringType(), False),
                                        StructField(
                                            "newValue",
                                            ArrayType(
                                                StructType(
                                                    [
                                                        StructField(
                                                            "contractTitleLineType",
                                                            StringType(),
                                                            False,
                                                        ),
                                                        StructField(
                                                            "contractTitleLine",
                                                            StringType(),
                                                        ),
                                                    ]
                                                )
                                            ),
                                            False,
                                        ),
                                        StructField("oldValue", NullType()),
                                    ]
                                ),
                                False,
                            ),
                            StructField(
                                "taxIdentifier",
                                StructType(
                                    [
                                        StructField("operation", StringType(), False),
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
                                            False,
                                        ),
                                        StructField("oldValue", NullType()),
                                    ]
                                ),
                                False,
                            ),
                            StructField(
                                "contractBranchCode",
                                StructType(
                                    [
                                        StructField("operation", StringType(), False),
                                        StructField("newValue", StringType()),
                                        StructField("oldValue", NullType()),
                                    ]
                                ),
                                False,
                            ),
                            StructField(
                                "contractCountry",
                                StructType(
                                    [
                                        StructField("operation", StringType(), False),
                                        StructField("newValue", StringType()),
                                        StructField("oldValue", NullType()),
                                    ]
                                ),
                                False,
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
                                                            "operation",
                                                            StringType(),
                                                            False,
                                                        ),
                                                        StructField(
                                                            "newValue", StringType()
                                                        ),
                                                        StructField(
                                                            "oldValue", NullType()
                                                        ),
                                                    ]
                                                ),
                                                False,
                                            ),
                                            StructField(
                                                "partyRelationshipType",
                                                StructType(
                                                    [
                                                        StructField(
                                                            "operation",
                                                            StringType(),
                                                            False,
                                                        ),
                                                        StructField(
                                                            "newValue", StringType()
                                                        ),
                                                        StructField(
                                                            "oldValue", NullType()
                                                        ),
                                                    ]
                                                ),
                                                False,
                                            ),
                                            StructField(
                                                "partyRelationStartDateTime",
                                                StructType(
                                                    [
                                                        StructField(
                                                            "operation",
                                                            StringType(),
                                                            False,
                                                        ),
                                                        StructField(
                                                            "newValue", TimestampType()
                                                        ),
                                                        StructField(
                                                            "oldValue", NullType()
                                                        ),
                                                    ]
                                                ),
                                                False,
                                            ),
                                            StructField(
                                                "partyAddress",
                                                StructType(
                                                    [
                                                        StructField(
                                                            "operation",
                                                            StringType(),
                                                            False,
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
                                                            False,
                                                        ),
                                                        StructField(
                                                            "oldValue", NullType()
                                                        ),
                                                    ]
                                                ),
                                            ),
                                        ]
                                    ),
                                    False,
                                ),
                            ),
                        ]
                    ),
                    False,
                ),
            ]
        )
