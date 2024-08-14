import pytest

from datetime import datetime

from pyspark.sql.types import Row

from lib.Utils import get_spark_session

from schema.AccountsSchema import AccountsSchema
from schema.TransformedAccountsSchema import TransformedAccountsSchema
from schema.JoinedPartyAddressesSchema import JoinedPartyAddressesSchema
from schema.JoinedAccountPartyAddressesSchema import JoinedAccountPartyAddressesSchema

from transform.AccountsTransformer import AccountsTransformer

from tests.test_parties_transformer import JOINED_ROW_WITH_PARTY_ADDRESS1


@pytest.fixture(scope="session")
def spark():
    return get_spark_session("LOCAL")


ROW_WITH_ACTIVE_ACCOUNT = Row(
    datetime.fromisoformat("2022-08-02"),
    1,
    "6982391060",
    "COH",
    datetime.fromisoformat("2018-03-24T13:56:45.000+05:30"),
    "Jane Doe",
    "John Doe",
    "EIN",
    "ABCD01234567898765",
    "ABCDEFG1",
    "Mexico",
)

ROW_WITH_ACTIVE_ACCOUNT_BUT_WITH_ONLY_ONE_LEGAL_TITLE = Row(
    datetime.fromisoformat("2022-08-02"),
    1,
    "6982391060",
    "COH",
    datetime.fromisoformat("2018-03-24T13:56:45.000+05:30"),
    "Jane Doe",
    None,
    "EIN",
    "ABCD01234567898765",
    "ABCDEFG1",
    "Mexico",
)

ROW_WITH_INACTIVE_ACCOUNT = Row(
    datetime.fromisoformat("2022-08-02"),
    0,
    "6982391061",
    "ADS",
    datetime.fromisoformat("2018-07-19T11:24:49.000+05:30"),
    "First Last",
    "Name1 Name2",
    "EIN",
    "ABCD12345678987656",
    "ABCDEFG2",
    "Mexico",
)

TRANSFORMED_ROW_WITH_ACTIVE_ACCOUNT = Row(
    "6982391060",
    {
        "operation": "INSERT",
        "newValue": "6982391060",
        "oldValue": None,
    },
    {
        "operation": "INSERT",
        "newValue": "COH",
        "oldValue": None,
    },
    {
        "operation": "INSERT",
        "newValue": datetime.fromisoformat("2018-03-24T13:56:45.000+05:30"),
        "oldValue": None,
    },
    {
        "operation": "INSERT",
        "newValue": [
            {
                "contractTitleLineType": "lgl_ttl_ln_1",
                "contractTitleLine": "Jane Doe",
            },
            {
                "contractTitleLineType": "lgl_ttl_ln_2",
                "contractTitleLine": "John Doe",
            },
        ],
        "oldValue": None,
    },
    {
        "operation": "INSERT",
        "newValue": {
            "taxIdType": "EIN",
            "taxId": "ABCD01234567898765",
        },
        "oldValue": None,
    },
    {
        "operation": "INSERT",
        "newValue": "ABCDEFG1",
        "oldValue": None,
    },
    {
        "operation": "INSERT",
        "newValue": "Mexico",
        "oldValue": None,
    },
)

TRANSFORMED_ROW_WITH_ACTIVE_ACCOUNT_BUT_WITH_ONLY_ONE_LEGAL_TITLE = Row(
    "6982391060",
    {
        "operation": "INSERT",
        "newValue": "6982391060",
        "oldValue": None,
    },
    {
        "operation": "INSERT",
        "newValue": "COH",
        "oldValue": None,
    },
    {
        "operation": "INSERT",
        "newValue": datetime.fromisoformat("2018-03-24T13:56:45.000+05:30"),
        "oldValue": None,
    },
    {
        "operation": "INSERT",
        "newValue": [
            {
                "contractTitleLineType": "lgl_ttl_ln_1",
                "contractTitleLine": "Jane Doe",
            },
        ],
        "oldValue": None,
    },
    {
        "operation": "INSERT",
        "newValue": {
            "taxIdType": "EIN",
            "taxId": "ABCD01234567898765",
        },
        "oldValue": None,
    },
    {
        "operation": "INSERT",
        "newValue": "ABCDEFG1",
        "oldValue": None,
    },
    {
        "operation": "INSERT",
        "newValue": "Mexico",
        "oldValue": None,
    },
)

JOINED_ROW_WITH_ACCOUNT_PARTY_ADDRESS = Row(
    "6982391060",
    {
        "operation": "INSERT",
        "newValue": "6982391060",
        "oldValue": None,
    },
    {
        "operation": "INSERT",
        "newValue": "COH",
        "oldValue": None,
    },
    {
        "operation": "INSERT",
        "newValue": datetime.fromisoformat("2018-03-24T13:56:45.000+05:30"),
        "oldValue": None,
    },
    {
        "operation": "INSERT",
        "newValue": [
            {
                "contractTitleLineType": "lgl_ttl_ln_1",
                "contractTitleLine": "Jane Doe",
            },
            {
                "contractTitleLineType": "lgl_ttl_ln_2",
                "contractTitleLine": "John Doe",
            },
        ],
        "oldValue": None,
    },
    {
        "operation": "INSERT",
        "newValue": {
            "taxIdType": "EIN",
            "taxId": "ABCD01234567898765",
        },
        "oldValue": None,
    },
    {
        "operation": "INSERT",
        "newValue": "ABCDEFG1",
        "oldValue": None,
    },
    {
        "operation": "INSERT",
        "newValue": "Mexico",
        "oldValue": None,
    },
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


def test_transform_dataframe_with_one_row(spark):

    expected = spark.createDataFrame(
        [TRANSFORMED_ROW_WITH_ACTIVE_ACCOUNT],
        schema=TransformedAccountsSchema.get_schema(),
    ).repartition(2)

    df = spark.createDataFrame(
        [ROW_WITH_ACTIVE_ACCOUNT], schema=AccountsSchema.get_schema()
    ).repartition(2)

    t = AccountsTransformer()
    actual = t.transform(df)

    assert actual.collect() == expected.collect()


def test_transform_dataframe_with_one_row_that_is_missing_one_of_the_legal_titles(
    spark,
):

    expected = spark.createDataFrame(
        [TRANSFORMED_ROW_WITH_ACTIVE_ACCOUNT_BUT_WITH_ONLY_ONE_LEGAL_TITLE],
        schema=TransformedAccountsSchema.get_schema(),
    ).repartition(2)

    df = spark.createDataFrame(
        [ROW_WITH_ACTIVE_ACCOUNT_BUT_WITH_ONLY_ONE_LEGAL_TITLE],
        schema=AccountsSchema.get_schema(),
    ).repartition(2)

    t = AccountsTransformer()
    actual = t.transform(df)

    assert actual.collect() == expected.collect()


def test_transform_dataframe_with_two_rows_that_only_keeps_one_active_account(spark):

    expected = spark.createDataFrame(
        [TRANSFORMED_ROW_WITH_ACTIVE_ACCOUNT],
        schema=TransformedAccountsSchema.get_schema(),
    ).repartition(2)

    df = spark.createDataFrame(
        [
            ROW_WITH_ACTIVE_ACCOUNT,
            ROW_WITH_INACTIVE_ACCOUNT,
        ],
        schema=AccountsSchema.get_schema(),
    ).repartition(2)

    t = AccountsTransformer()
    actual = t.transform(df)

    assert actual.collect() == expected.collect()


def test_join_one_transformed_party_address(spark):

    expected = spark.createDataFrame(
        [JOINED_ROW_WITH_ACCOUNT_PARTY_ADDRESS],
        schema=JoinedAccountPartyAddressesSchema.get_schema(),
    ).repartition(2)

    transformed_accounts = spark.createDataFrame(
        [TRANSFORMED_ROW_WITH_ACTIVE_ACCOUNT],
        schema=TransformedAccountsSchema.get_schema(),
    ).repartition(2)

    joined_party_address = spark.createDataFrame(
        [JOINED_ROW_WITH_PARTY_ADDRESS1], schema=JoinedPartyAddressesSchema.get_schema()
    ).repartition(2)

    t = AccountsTransformer()
    actual = t.join(transformed_accounts, joined_party_address)

    assert actual.collect() == expected.collect()
