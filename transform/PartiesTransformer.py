from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col

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
