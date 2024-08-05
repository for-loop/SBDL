from pyspark.sql import DataFrame


class DataLoader:

    def __init__(self, spark):
        self.spark = spark

    def load(self, path) -> DataFrame:
        df = (
            self.spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(path)
        )
        return df
