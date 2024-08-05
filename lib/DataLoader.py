from pyspark.sql import DataFrame


class DataLoader:

    def __init__(self, spark, conf):
        self.spark = spark
        self.conf = conf

    def load(self, path_or_table) -> DataFrame:
        if self.conf["enable.hive"] == "true":
            return self.spark.sql(f"SELECT * FROM {path_or_table}")

        return (
            self.spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(path_or_table)
        )
