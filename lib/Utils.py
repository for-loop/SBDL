from pyspark.sql import SparkSession
from conf.ConfigLoader import get_spark_conf


def get_spark_session(env):
    if env == "LOCAL":
        return (
            SparkSession.builder.config(conf=get_spark_conf(env))
            .config(
                "spark.driver.extraJavaOptions",
                "-Dlog4j.configuration=file:log4j.properties",
            )
            .master("local[2]")
            .enableHiveSupport()
            .getOrCreate()
        )

    return (
        SparkSession.builder.config(conf=get_spark_conf(env))
        .enableHiveSupport()
        .getOrCreate()
    )
