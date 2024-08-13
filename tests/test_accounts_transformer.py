import pytest

from datetime import datetime

from pyspark.sql.types import Row

from lib.Utils import get_spark_session

from schema.AccountsSchema import AccountsSchema
from schema.TransformedAccountsSchema import TransformedAccountsSchema

from transform.AccountsTransformer import AccountsTransformer


@pytest.fixture(scope="session")
def spark():
    return get_spark_session("LOCAL")


def test_transform_dataframe_with_one_row(spark):

    expected_rows = [
        Row(
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
    ]

    expected = spark.createDataFrame(
        expected_rows, schema=TransformedAccountsSchema.get_schema()
    ).repartition(2)

    rows = [
        Row(
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
    ]

    df = spark.createDataFrame(rows, schema=AccountsSchema.get_schema()).repartition(2)

    t = AccountsTransformer(spark)
    actual = t.transform(df)

    assert actual.collect() == expected.collect()


def test_transform_dataframe_with_one_row_that_is_missing_one_of_the_legal_titles(
    spark,
):

    expected_rows = [
        Row(
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
    ]

    expected = spark.createDataFrame(
        expected_rows, schema=TransformedAccountsSchema.get_schema()
    ).repartition(2)

    rows = [
        Row(
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
    ]

    df = spark.createDataFrame(rows, schema=AccountsSchema.get_schema()).repartition(2)

    t = AccountsTransformer(spark)
    actual = t.transform(df)

    assert actual.collect() == expected.collect()


def test_transform_dataframe_with_two_rows_that_only_keeps_one_active_account(spark):

    expected_rows = [
        Row(
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
    ]

    expected = spark.createDataFrame(
        expected_rows, schema=TransformedAccountsSchema.get_schema()
    ).repartition(2)

    rows = [
        Row(
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
        ),
        Row(
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
        ),
    ]

    df = spark.createDataFrame(rows, schema=AccountsSchema.get_schema()).repartition(2)

    t = AccountsTransformer(spark)
    actual = t.transform(df)

    assert actual.collect() == expected.collect()
