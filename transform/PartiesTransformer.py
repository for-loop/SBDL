from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, collect_list, struct

from transform.Transformer import Transformer


class PartiesTransformer(Transformer):

    def transform(self, df: DataFrame) -> DataFrame:
        return df.select(
            "account_id",
            "party_id",
            self._add_insert(col("party_id"), "partyIdentifier"),
            self._add_insert(col("relation_type"), "partyRelationshipType"),
            self._add_insert(col("relation_start_date"), "partyRelationStartDateTime"),
        )

    def join(self, party_df: DataFrame, address_df: DataFrame) -> DataFrame:
        return (
            party_df.join(address_df, "party_id", "left_outer")
            .groupBy("account_id")
            .agg(
                collect_list(
                    struct(
                        "partyIdentifier",
                        "partyRelationshipType",
                        "partyRelationStartDateTime",
                        "partyAddress",
                    ).alias("partyDetails")
                ).alias("partyRelations")
            )
        )
