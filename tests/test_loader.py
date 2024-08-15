import pytest

import os
import shutil

from pyspark.sql.types import Row

from lib.Utils import get_spark_session

from load.LoaderFactoryImpl import LoaderFactoryImpl
from load.JsonLoader import JsonLoader

from exceptions.MissingTargetFormatError import MissingTargetFormatError
from exceptions.UnsupportedTargetFormatError import UnsupportedTargetFormatError
from exceptions.MissingTargetLocationError import MissingTargetLocationError


@pytest.fixture(scope="session")
def spark():
    return get_spark_session("LOCAL")


def test_throws_when_target_format_is_missing_in_conf():
    conf = {}
    f = LoaderFactoryImpl(conf)
    with pytest.raises(MissingTargetFormatError):
        f.make_loader()


def test_throws_when_target_format_is_unsupported():
    conf = {"target.format": "unsupported_format"}
    f = LoaderFactoryImpl(conf)
    with pytest.raises(UnsupportedTargetFormatError):
        f.make_loader()


def test_loader_throws_when_target_location_is_missing_in_conf(spark):
    conf = {"target.format": "json"}
    f = LoaderFactoryImpl(conf)
    l = f.make_loader()

    rows = [Row(1, "Name1"), Row(2, "Name2")]
    df = spark.createDataFrame(rows).toDF("id", "name").repartition(2)

    assert isinstance(l, JsonLoader)
    with pytest.raises(MissingTargetLocationError):
        l.load(df)


def test_loader_writes_json(spark):
    conf = {"target.format": "json", "target.location": "test_data/loader_data/"}
    f = LoaderFactoryImpl(conf)
    l = f.make_loader()

    rows = [Row(1, "Name1"), Row(2, "Name2")]
    df = spark.createDataFrame(rows).toDF("id", "name").repartition(2)

    assert isinstance(l, JsonLoader)

    l.load(df)

    assert os.path.exists(f"{conf['target.location']}_SUCCESS") == True
