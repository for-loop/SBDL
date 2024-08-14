import pytest

from lib.Utils import get_spark_session
from transform.TransformerFacade import TransformerFacade

from schema.AccountsSchema import AccountsSchema
from schema.PartiesSchema import PartiesSchema
from schema.AddressesSchema import AddressesSchema
from schema.TransformedSchema import TransformedSchema

from tests.test_accounts_transformer import ROW_WITH_ACTIVE_ACCOUNT
from tests.test_parties_transformer import ROW_WITH_PARTY1
from tests.test_addresses_transformer import ROW_WITH_ADDRESS1


@pytest.fixture(scope="session")
def spark():
    return get_spark_session("LOCAL")


def test_transform_in_transformer_facade(spark):
    accounts_df = spark.createDataFrame(
        [ROW_WITH_ACTIVE_ACCOUNT], schema=AccountsSchema.get_schema()
    ).repartition(2)
    parties_df = spark.createDataFrame(
        [ROW_WITH_PARTY1], schema=PartiesSchema.get_schema()
    ).repartition(2)
    addresses_df = spark.createDataFrame(
        [ROW_WITH_ADDRESS1], schema=AddressesSchema.get_schema()
    ).repartition(2)

    tf = TransformerFacade()
    df = tf.transform(accounts_df, parties_df, addresses_df)

    assert df.schema.fields == TransformedSchema.get_schema().fields


def test_transform_all_test_data_returns_eight_rows(spark):

    accounts_df = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(AccountsSchema.get_schema())
        .load("test_data/accounts/")
    )
    parties_df = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(PartiesSchema.get_schema())
        .load("test_data/parties/")
    )
    addresses_df = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(AddressesSchema.get_schema())
        .load("test_data/party_address/")
    )

    tf = TransformerFacade()
    df = tf.transform(accounts_df, parties_df, addresses_df)

    assert df.count() == 8
