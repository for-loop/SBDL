from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    struct,
    array,
    expr,
)


from datetime import datetime

from transform.Transformer import Transformer


class HeadersTransformer(Transformer):

    def transform(self, df: DataFrame) -> DataFrame:

        MAJOR_VERS = 1
        MINOR_VERS = 0

        return df.select(
            struct(
                expr("uuid()").alias("eventIdentifier"),
                lit("SBDL-Contract").alias("eventType"),
                lit(MAJOR_VERS).alias("majorSchemaVersion"),
                lit(MINOR_VERS).alias("minorSchemaVersion"),
                lit(datetime.now()).alias("eventDateTime"),
            ).alias("eventHeader"),
            array(
                struct(
                    lit("contractIdentifier").alias("keyField"),
                    col("account_id").alias("keyValue"),
                )
            ).alias("keys"),
            struct(
                col("contractIdentifier"),
                col("sourceSystemIdentifier"),
                col("contactStartDateTime"),
                col("contractTitle"),
                col("taxIdentifier"),
                col("contractBranchCode"),
                col("contractCountry"),
                col("partyRelations"),
            ).alias("payload"),
        )
