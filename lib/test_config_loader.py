import pytest

from lib.Utils import get_spark_session
from lib import ConfigLoader

LOCAL_ENV = "LOCAL"


def test_sbdl_config_is_read_correctly_for_local():
    conf = ConfigLoader.get_config(LOCAL_ENV)
    assert conf["kafka.topic"] == "sbdl_kafka_cloud"


def test_spark_config_is_read_correctly_for_local():
    spark_conf = ConfigLoader.get_spark_conf(LOCAL_ENV)
    assert spark_conf.get("spark.app.name") == "sbdl-local"
