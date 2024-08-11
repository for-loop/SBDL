import pytest

from pyspark.sql.types import Row
from pyspark.sql.functions import col, lit, struct
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    NullType,
    TimestampType,
)

from datetime import datetime

from lib.Utils import get_spark_session
from lib.Transformer import Transformer


@pytest.fixture(scope="session")
def spark():
    return get_spark_session("LOCAL")


def test_transform_dataframe_with_one_row(spark):
    expected_schema = StructType(
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

    expected_row = [
        Row(
            "6982391060",
            "9823462810",
            {"operation": "INSERT", "newValue": "9823462810", "oldValue": None},
            {"operation": "INSERT", "newValue": "F-N", "oldValue": None},
            {
                "operation": "INSERT",
                "newValue": datetime.fromisoformat("2019-07-29T06:21:32.000+05:30"),
                "oldValue": None,
            },
        ),
    ]

    expected = spark.createDataFrame(expected_row, schema=expected_schema)

    t = Transformer(spark)

    row = [
        Row(
            "6982391060",
            "9823462810",
            "F-N",
            datetime.fromisoformat("2019-07-29T06:21:32.000+05:30"),
        ),
    ]

    df = (
        spark.createDataFrame(row)
        .toDF("account_id", "party_id", "relation_type", "relation_start_date")
        .repartition(2)
    )
    actual = t.transform(df)

    assert actual.collect() == expected.collect()
