import pytest

import os

from pyspark.sql.types import Row

from lib.Utils import get_spark_session

from load.LoaderFacade import LoaderFacade


@pytest.fixture(scope="session")
def spark():
    return get_spark_session("LOCAL")


def test_load_does_not_throw(spark):
    conf = {"target.format": "json", "target.location": "test_data/loader_data/"}

    rows = [Row(1, "Name1"), Row(2, "Name2")]

    df = spark.createDataFrame(rows).toDF("id", "name").repartition(2)

    loader = LoaderFacade(conf)
    loader.load(df)

    assert os.path.exists(f"{conf['target.location']}_SUCCESS") == True
