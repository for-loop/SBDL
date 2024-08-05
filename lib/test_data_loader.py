import pytest

from pyspark.sql import DataFrame
from lib.Utils import get_spark_session
from lib.DataLoader import DataLoader


@pytest.fixture(scope="session")
def spark():
    return get_spark_session("LOCAL")


def test_load_creates_dataframe_of_nine_rows(spark):
    d = DataLoader(spark)
    df = d.load("test_data/accounts/")
    assert isinstance(df, DataFrame)
    assert df.count() == 9
