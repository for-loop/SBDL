from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    NullType,
    ArrayType,
)


class TransformedAccountsSchema:

    @staticmethod
    def get_schema():
        return StructType(
            [
                StructField("account_id", StringType()),
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
                                                "contractTitleLineType", StringType()
                                            ),
                                            StructField(
                                                "contractTitleLine", StringType()
                                            ),
                                        ]
                                    ),
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
                                        StructField("taxIdType", StringType()),
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
            ]
        )
