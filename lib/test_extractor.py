import pytest

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    LongType,
    StringType,
    DateType,
)

from lib.Utils import get_spark_session

from lib.CsvExtractor import CsvExtractor
from lib.HiveExtractor import HiveExtractor
from lib.ExtractorFactoryImpl import ExtractorFactoryImpl
from lib.MissingSchemaError import MissingSchemaError
from lib.MissingSourceLocationError import MissingSourceLocationError

from lib.AccountsConfig import AccountsConfig
from lib.PartiesConfig import PartiesConfig


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


def test_extract_creates_dataframe_of_nine_rows(spark):
    conf = {
        "enable.hive": "false",
        "accounts.source.location": "test_data/accounts/",
        "accounts.schema": "load_date date,active_ind int,account_id string,source_sys string,account_start_date timestamp,legal_title_1 string,legal_title_2 string,tax_id_type string,tax_id string,branch_code string,country string",
    }
    f = ExtractorFactoryImpl(conf)
    e = f.make_extractor(spark)
    accounts_config = AccountsConfig(conf)
    df = e.extract(accounts_config)

    assert isinstance(df, DataFrame)
    assert isinstance(e, CsvExtractor)
    assert df.count() == 9


def test_extract_from_hive_table_when_enable_hive_config_is_true(spark, hive):
    conf = {
        "enable.hive": "true",
        "accounts.source.location": "test_db.accounts",
        "accounts.schema": "",
    }
    f = ExtractorFactoryImpl(conf)
    e = f.make_extractor(spark)
    accounts_config = AccountsConfig(conf)
    df = e.extract(accounts_config)

    assert isinstance(e, HiveExtractor)
    assert df.count() == 2


def test_hive_extractor_throws_when_source_location_is_empty_string(spark):
    conf = {
        "enable.hive": "true",
        "accounts.source.location": "",
        "accounts.schema": "",
    }
    f = ExtractorFactoryImpl(conf)
    e = f.make_extractor(spark)
    accounts_config = AccountsConfig(conf)

    with pytest.raises(MissingSourceLocationError):
        df = e.extract(accounts_config)


def test_csv_extractor_throws_when_source_location_is_empty_string(spark):
    conf = {
        "enable.hive": "false",
        "accounts.source.location": "",
        "accounts.schema": "load_date date,active_ind int,account_id string,source_sys string,account_start_date timestamp,legal_title_1 string,legal_title_2 string,tax_id_type string,tax_id string,branch_code string,country string",
    }
    f = ExtractorFactoryImpl(conf)
    e = f.make_extractor(spark)
    accounts_config = AccountsConfig(conf)

    with pytest.raises(MissingSourceLocationError):
        df = e.extract(accounts_config)


def test_csv_extractor_throws_when_schema_is_missing(spark):
    conf = {
        "enable.hive": "false",
        "accounts.source.location": "test_data/accounts/",
        "accounts.schema": "",
    }
    f = ExtractorFactoryImpl(conf)
    e = f.make_extractor(spark)
    accounts_config = AccountsConfig(conf)

    with pytest.raises(MissingSchemaError):
        df = e.extract(accounts_config)


def test_schema_of_parties_dataframe_is_as_expected(spark):
    expected_schema = [
        StructField("load_date", DateType(), True),
        StructField("account_id", StringType(), True),
        StructField("party_id", StringType(), True),
        StructField("relation_type", StringType(), True),
        StructField("relation_start_date", TimestampType(), True),
    ]

    conf = {
        "enable.hive": "false",
        "party.source.location": "test_data/parties/",
        "party.schema": "load_date date,account_id string,party_id string,relation_type string,relation_start_date timestamp",
    }
    ef = ExtractorFactoryImpl(conf)
    e = ef.make_extractor(spark)
    parties_config = PartiesConfig(conf)
    df = e.extract(parties_config)

    assert df.schema.fields == expected_schema
