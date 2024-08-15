import pytest

from lib.Utils import get_spark_session
from conf import ConfigLoader

LOCAL_ENV = "LOCAL"


def test_sbdl_config_is_read_correctly_for_local():
    conf = ConfigLoader.get_config(LOCAL_ENV)
    assert conf["kafka.topic"] == "sbdl_kafka_cloud"


def test_spark_config_is_read_correctly_for_local():
    spark_conf = ConfigLoader.get_spark_conf(LOCAL_ENV)
    assert spark_conf.get("spark.app.name") == "sbdl-local"


def test_get_data_filter_throws_when_conf_item_does_not_exist():
    with pytest.raises(KeyError):
        ConfigLoader.get_data_filter(LOCAL_ENV, "CONF_ITEM_DOES_NOT_EXIST")


def test_get_data_filter_reads_where_condition_correctly_for_local():
    assert ConfigLoader.get_data_filter(LOCAL_ENV, "account.filter") == "active_ind = 1"


def test_get_data_filter_returns_string_true_when_conf_item_is_empty_string():
    conf = ConfigLoader.get_config(LOCAL_ENV)
    conf_item_with_empty_string = "party.filter"
    assert len(conf[conf_item_with_empty_string]) == 0
    assert (
        ConfigLoader.get_data_filter(LOCAL_ENV, conf_item_with_empty_string) == "true"
    )
