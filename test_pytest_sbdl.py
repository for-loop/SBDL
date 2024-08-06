import pytest

from lib.Utils import get_spark_session


@pytest.fixture(scope="session")
def spark():
    return get_spark_session("LOCAL")


def test_spark_version_is_3_3_0(spark):
    assert spark.version == "3.3.0"


def test_spark_session_is_local(spark):
    assert "local" in spark.sparkContext.getConf().get("spark.master")


def test_spark_conf_is_loaded_from_file(spark):
    assert spark.sparkContext.getConf().get("spark.app.name") == "sbdl-local"
