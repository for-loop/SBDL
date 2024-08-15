from pyspark.sql.dataframe import DataFrame

from load.Loader import Loader


class JsonLoader(Loader):

    def load(self, df: DataFrame) -> None:

        target_location = self.get_target_location("target.location")

        return df.write.format("json").mode("overwrite").save(target_location)
