import pytest

from lib.Utils import get_spark_session

from schema.JoinedAccountPartyAddressesSchema import JoinedAccountPartyAddressesSchema
from schema.TransformedSchema import TransformedSchema

from transform.HeadersTransformer import HeadersTransformer

from tests.test_accounts_transformer import JOINED_ROW_WITH_ACCOUNT_PARTY_ADDRESS


@pytest.fixture(scope="session")
def spark():
    return get_spark_session("LOCAL")


def test_transform_prepends_header_to_dataframe_with_one_row(spark):

    data_df = spark.createDataFrame(
        [JOINED_ROW_WITH_ACCOUNT_PARTY_ADDRESS],
        schema=JoinedAccountPartyAddressesSchema.get_schema(),
    ).repartition(2)

    t = HeadersTransformer(spark)
    df = t.transform(data_df)

    assert df.schema.fields == TransformedSchema.get_schema().fields
