import pytest

from pyspark.sql.types import Row

from datetime import datetime

from lib.Utils import get_spark_session
from lib.AddressesTransformer import AddressesTransformer

from schema.TransformedAddressesSchema import TransformedAddressesSchema
from schema.AddressesSchema import AddressesSchema


@pytest.fixture(scope="session")
def spark():
    return get_spark_session("LOCAL")


def test_transform_dataframe_with_one_row(spark):

    expected_rows = [
        Row(
            "9823462810",
            {
                "operation": "INSERT",
                "newValue": {
                    "addressLine1": "45229 Drake Route",
                    "addressLine2": "13306 Corey Point",
                    "addressCity": "Shanefort",
                    "addressPostalCode": "77163",
                    "addressCountry": "Canada",
                    "addressStartDate": datetime.fromisoformat("2019-02-26"),
                },
                "oldValue": None,
            },
        )
    ]

    expected = spark.createDataFrame(
        expected_rows, schema=TransformedAddressesSchema.get_schema()
    ).repartition(2)

    rows = [
        Row(
            datetime.fromisoformat("2022-08-02"),
            "9823462810",
            "45229 Drake Route",
            "13306 Corey Point",
            "Shanefort",
            "77163",
            "Canada",
            datetime.fromisoformat("2019-02-26"),
        )
    ]

    df = spark.createDataFrame(rows, schema=AddressesSchema.get_schema()).repartition(2)

    t = AddressesTransformer(spark)
    actual = t.transform(df)

    assert actual.collect() == expected.collect()


def test_transform_dataframe_with_two_rows(spark):

    expected_rows = [
        Row(
            "9823462810",
            {
                "operation": "INSERT",
                "newValue": {
                    "addressLine1": "45229 Drake Route",
                    "addressLine2": "13306 Corey Point",
                    "addressCity": "Shanefort",
                    "addressPostalCode": "77163",
                    "addressCountry": "Canada",
                    "addressStartDate": datetime.fromisoformat("2019-02-26"),
                },
                "oldValue": None,
            },
        ),
        Row(
            "9823462811",
            {
                "operation": "INSERT",
                "newValue": {
                    "addressLine1": "361 Robinson Green",
                    "addressLine2": "3511 Rebecca Mission",
                    "addressCity": "North Tyler",
                    "addressPostalCode": "34118",
                    "addressCountry": "Canada",
                    "addressStartDate": datetime.fromisoformat("2018-01-28"),
                },
                "oldValue": None,
            },
        ),
    ]

    expected = spark.createDataFrame(
        expected_rows, schema=TransformedAddressesSchema.get_schema()
    ).repartition(2)

    rows = [
        Row(
            datetime.fromisoformat("2022-08-02"),
            "9823462810",
            "45229 Drake Route",
            "13306 Corey Point",
            "Shanefort",
            "77163",
            "Canada",
            datetime.fromisoformat("2019-02-26"),
        ),
        Row(
            datetime.fromisoformat("2022-08-02"),
            "9823462811",
            "361 Robinson Green",
            "3511 Rebecca Mission",
            "North Tyler",
            "34118",
            "Canada",
            datetime.fromisoformat("2018-01-28"),
        ),
    ]

    df = spark.createDataFrame(rows, schema=AddressesSchema.get_schema()).repartition(2)

    t = AddressesTransformer(spark)
    actual = t.transform(df)

    assert actual.collect() == expected.collect()
