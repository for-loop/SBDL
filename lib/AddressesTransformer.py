from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, struct

from lib.Transformer import Transformer


class AddressesTransformer(Transformer):

    def transform(self, df: DataFrame) -> DataFrame:
        address = struct(
            col("address_line_1").alias("addressLine1"),
            col("address_line_2").alias("addressLine2"),
            col("city").alias("addressCity"),
            col("postal_code").alias("addressPostalCode"),
            col("country_of_address").alias("addressCountry"),
            col("address_start_date").alias("addressStartDate"),
        )

        return df.select("party_id", self._add_insert(address, "partyAddress"))
