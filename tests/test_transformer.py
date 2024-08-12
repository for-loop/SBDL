import pytest

from pyspark.sql.types import Row
from pyspark.sql.functions import col, lit, struct

from datetime import datetime

from lib.Utils import get_spark_session
from lib.Transformer import Transformer

from schema.PartiesSchema import PartiesSchema
from schema.RelationsSchema import RelationsSchema


@pytest.fixture(scope="session")
def spark():
    return get_spark_session("LOCAL")


def test_transform_dataframe_with_one_row(spark):

    expected_rows = [
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

    expected = spark.createDataFrame(
        expected_rows, schema=RelationsSchema.get_schema()
    ).repartition(2)

    rows = [
        Row(
            datetime.fromisoformat("2022-08-02"),
            "6982391060",
            "9823462810",
            "F-N",
            datetime.fromisoformat("2019-07-29T06:21:32.000+05:30"),
        ),
    ]

    df = spark.createDataFrame(rows, schema=PartiesSchema.get_schema()).repartition(2)

    t = Transformer(spark)
    actual = t.transform(df)

    assert actual.collect() == expected.collect()


def test_transform_dataframe_with_two_rows(spark):

    expected_rows = [
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
        Row(
            "6982391061",
            "9823462811",
            {"operation": "INSERT", "newValue": "9823462811", "oldValue": None},
            {"operation": "INSERT", "newValue": "F-N", "oldValue": None},
            {
                "operation": "INSERT",
                "newValue": datetime.fromisoformat("2018-08-31T05:27:22.000+05:30"),
                "oldValue": None,
            },
        ),
    ]

    expected = spark.createDataFrame(
        expected_rows, schema=RelationsSchema.get_schema()
    ).repartition(2)

    rows = [
        Row(
            datetime.fromisoformat("2022-08-02"),
            "6982391060",
            "9823462810",
            "F-N",
            datetime.fromisoformat("2019-07-29T06:21:32.000+05:30"),
        ),
        Row(
            datetime.fromisoformat("2022-08-02"),
            "6982391061",
            "9823462811",
            "F-N",
            datetime.fromisoformat("2018-08-31T05:27:22.000+05:30"),
        ),
    ]

    df = spark.createDataFrame(rows, schema=PartiesSchema.get_schema()).repartition(2)

    t = Transformer(spark)
    actual = t.transform(df)

    assert actual.collect() == expected.collect()
