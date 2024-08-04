import pytest

from lib.Utils import get_spark_session


@pytest.fixture(scope="session")
def spark():
    return get_spark_session("LOCAL")


def test_spark_version_is_3_3_0(spark):
    assert spark.version == "3.3.0"
