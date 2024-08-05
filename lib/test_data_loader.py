import pytest

from pyspark.sql import DataFrame
from lib.Utils import get_spark_session
from lib.DataLoader import DataLoader


@pytest.fixture(scope="session")
def spark():
    return get_spark_session("LOCAL")


@pytest.fixture(scope="module")
def hive(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS test_db")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS test_db.accounts (
            id INT,
            name STRING
        )
        """
    )
    spark.sql(
        """
        INSERT INTO test_db.accounts (id, name) VALUES (1, 'Dummy'), (2, 'Dummy2')
        """
    )

    yield

    spark.sql("DROP TABLE test_db.accounts")
    spark.sql("DROP DATABASE test_db")


def test_load_creates_dataframe_of_nine_rows(spark):
    conf = {"enable.hive": "false"}
    d = DataLoader(spark, conf)
    df = d.load("test_data/accounts/")
    assert isinstance(df, DataFrame)
    assert df.count() == 9


def test_load_from_hive_table_when_enable_hive_config_is_true(spark, hive):
    conf = {"enable.hive": "true"}
    d = DataLoader(spark, conf)
    df = d.load("test_db.accounts")
    assert df.count() == 2
