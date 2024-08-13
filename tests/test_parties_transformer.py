import pytest

from pyspark.sql.types import Row
from pyspark.sql.functions import col, lit, struct

from datetime import datetime

from lib.Utils import get_spark_session

from transform.PartiesTransformer import PartiesTransformer

from schema.PartiesSchema import PartiesSchema
from schema.TransformedPartiesSchema import TransformedPartiesSchema
from schema.TransformedAddressesSchema import TransformedAddressesSchema
from schema.JoinedPartyAddressesSchema import JoinedPartyAddressesSchema

from tests.test_addresses_transformer import (
    TRANSFORMED_ROW_WITH_ADDRESS1,
    TRANSFORMED_ROW_WITH_ADDRESS2,
)


@pytest.fixture(scope="session")
def spark():
    return get_spark_session("LOCAL")


ROW_WITH_PARTY1 = Row(
    datetime.fromisoformat("2022-08-02"),
    "6982391060",
    "9823462810",
    "F-N",
    datetime.fromisoformat("2019-07-29T06:21:32.000+05:30"),
)

ROW_WITH_PARTY2 = Row(
    datetime.fromisoformat("2022-08-02"),
    "6982391060",
    "9823462811",
    "F-N",
    datetime.fromisoformat("2018-08-31T05:27:22.000+05:30"),
)

TRANSFORMED_ROW_WITH_PARTY1 = Row(
    "6982391060",
    "9823462810",
    {"operation": "INSERT", "newValue": "9823462810", "oldValue": None},
    {"operation": "INSERT", "newValue": "F-N", "oldValue": None},
    {
        "operation": "INSERT",
        "newValue": datetime.fromisoformat("2019-07-29T06:21:32.000+05:30"),
        "oldValue": None,
    },
)

TRANSFORMED_ROW_WITH_PARTY2 = Row(
    "6982391060",
    "9823462811",
    {"operation": "INSERT", "newValue": "9823462811", "oldValue": None},
    {"operation": "INSERT", "newValue": "F-N", "oldValue": None},
    {
        "operation": "INSERT",
        "newValue": datetime.fromisoformat("2018-08-31T05:27:22.000+05:30"),
        "oldValue": None,
    },
)

JOINED_ROW_WITH_PARTY_ADDRESS1 = Row(
    "6982391060",
    [
        {
            "partyIdentifier": {
                "operation": "INSERT",
                "newValue": "9823462810",
                "oldValue": None,
            },
            "partyRelationshipType": {
                "operation": "INSERT",
                "newValue": "F-N",
                "oldValue": None,
            },
            "partyRelationStartDateTime": {
                "operation": "INSERT",
                "newValue": datetime.fromisoformat("2019-07-29T06:21:32.000+05:30"),
                "oldValue": None,
            },
            "partyAddress": {
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
        }
    ],
)

JOINED_ROW_WITH_PARTY_ADDRESSES_OF_COMMON_ACCOUNT_ID = Row(
    "6982391060",
    [
        {
            "partyIdentifier": {
                "operation": "INSERT",
                "newValue": "9823462810",
                "oldValue": None,
            },
            "partyRelationshipType": {
                "operation": "INSERT",
                "newValue": "F-N",
                "oldValue": None,
            },
            "partyRelationStartDateTime": {
                "operation": "INSERT",
                "newValue": datetime.fromisoformat("2019-07-29T06:21:32.000+05:30"),
                "oldValue": None,
            },
            "partyAddress": {
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
        },
        {
            "partyIdentifier": {
                "operation": "INSERT",
                "newValue": "9823462811",
                "oldValue": None,
            },
            "partyRelationshipType": {
                "operation": "INSERT",
                "newValue": "F-N",
                "oldValue": None,
            },
            "partyRelationStartDateTime": {
                "operation": "INSERT",
                "newValue": datetime.fromisoformat("2018-08-31T05:27:22.000+05:30"),
                "oldValue": None,
            },
            "partyAddress": {
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
        },
    ],
)


def test_transform_dataframe_with_one_row(spark):

    expected = spark.createDataFrame(
        [TRANSFORMED_ROW_WITH_PARTY1], schema=TransformedPartiesSchema.get_schema()
    ).repartition(2)

    df = spark.createDataFrame(
        [ROW_WITH_PARTY1], schema=PartiesSchema.get_schema()
    ).repartition(2)

    t = PartiesTransformer(spark)
    actual = t.transform(df)

    assert actual.collect() == expected.collect()


def test_transform_dataframe_with_two_rows(spark):

    expected = spark.createDataFrame(
        [
            TRANSFORMED_ROW_WITH_PARTY1,
            TRANSFORMED_ROW_WITH_PARTY2,
        ],
        schema=TransformedPartiesSchema.get_schema(),
    ).repartition(2)

    df = spark.createDataFrame(
        [
            ROW_WITH_PARTY1,
            ROW_WITH_PARTY2,
        ],
        schema=PartiesSchema.get_schema(),
    ).repartition(2)

    t = PartiesTransformer(spark)
    actual = t.transform(df)

    assert actual.collect() == expected.collect()


def test_join_one_transformed_address(spark):

    expected = spark.createDataFrame(
        [JOINED_ROW_WITH_PARTY_ADDRESS1],
        schema=JoinedPartyAddressesSchema.get_schema(),
    ).repartition(2)

    transformed_parties = spark.createDataFrame(
        [TRANSFORMED_ROW_WITH_PARTY1], schema=TransformedPartiesSchema.get_schema()
    ).repartition(2)

    transformed_addresses = spark.createDataFrame(
        [TRANSFORMED_ROW_WITH_ADDRESS1], schema=TransformedAddressesSchema.get_schema()
    ).repartition(2)

    t = PartiesTransformer(spark)
    actual = t.join(transformed_parties, transformed_addresses)

    assert actual.collect() == expected.collect()


def test_join_two_transformed_addresses_with_common_account_id(spark):

    expected = spark.createDataFrame(
        [JOINED_ROW_WITH_PARTY_ADDRESSES_OF_COMMON_ACCOUNT_ID],
        schema=JoinedPartyAddressesSchema.get_schema(),
    ).repartition(2)

    transformed_parties = spark.createDataFrame(
        [TRANSFORMED_ROW_WITH_PARTY1, TRANSFORMED_ROW_WITH_PARTY2],
        schema=TransformedPartiesSchema.get_schema(),
    ).repartition(2)

    transformed_addresses = spark.createDataFrame(
        [TRANSFORMED_ROW_WITH_ADDRESS1, TRANSFORMED_ROW_WITH_ADDRESS2],
        schema=TransformedAddressesSchema.get_schema(),
    ).repartition(2)

    t = PartiesTransformer(spark)
    actual = t.join(transformed_parties, transformed_addresses)

    assert actual.collect() == expected.collect()
