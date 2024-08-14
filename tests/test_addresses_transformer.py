import pytest

from pyspark.sql.types import Row

from datetime import datetime

from lib.Utils import get_spark_session

from transform.AddressesTransformer import AddressesTransformer

from schema.TransformedAddressesSchema import TransformedAddressesSchema
from schema.AddressesSchema import AddressesSchema

ROW_WITH_ADDRESS1 = Row(
    datetime.fromisoformat("2022-08-02"),
    "9823462810",
    "45229 Drake Route",
    "13306 Corey Point",
    "Shanefort",
    "77163",
    "Canada",
    datetime.fromisoformat("2019-02-26"),
)

ROW_WITH_ADDRESS2 = Row(
    datetime.fromisoformat("2022-08-02"),
    "9823462811",
    "361 Robinson Green",
    "3511 Rebecca Mission",
    "North Tyler",
    "34118",
    "Canada",
    datetime.fromisoformat("2018-01-28"),
)

TRANSFORMED_ROW_WITH_ADDRESS1 = Row(
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

TRANSFORMED_ROW_WITH_ADDRESS2 = Row(
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
)


@pytest.fixture(scope="session")
def spark():
    return get_spark_session("LOCAL")


def test_transform_dataframe_with_one_row(spark):

    expected = spark.createDataFrame(
        [TRANSFORMED_ROW_WITH_ADDRESS1], schema=TransformedAddressesSchema.get_schema()
    ).repartition(2)

    df = spark.createDataFrame(
        [ROW_WITH_ADDRESS1], schema=AddressesSchema.get_schema()
    ).repartition(2)

    t = AddressesTransformer()
    actual = t.transform(df)

    assert actual.collect() == expected.collect()


def test_transform_dataframe_with_two_rows(spark):

    expected = spark.createDataFrame(
        [
            TRANSFORMED_ROW_WITH_ADDRESS1,
            TRANSFORMED_ROW_WITH_ADDRESS2,
        ],
        schema=TransformedAddressesSchema.get_schema(),
    ).repartition(2)

    df = spark.createDataFrame(
        [
            ROW_WITH_ADDRESS1,
            ROW_WITH_ADDRESS2,
        ],
        schema=AddressesSchema.get_schema(),
    ).repartition(2)

    t = AddressesTransformer()
    actual = t.transform(df)

    assert actual.collect() == expected.collect()
