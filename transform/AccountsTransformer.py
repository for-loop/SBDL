from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, struct, array, when, isnull, lit, filter

from transform.Transformer import Transformer


class AccountsTransformer(Transformer):

    def transform(self, df: DataFrame) -> DataFrame:

        contract_title_array = array(
            when(
                ~isnull("legal_title_1"),
                struct(
                    lit("lgl_ttl_ln_1").alias("contractTitleLineType"),
                    col("legal_title_1").alias("contractTitleLine"),
                ).alias("contractTitle"),
            ),
            when(
                ~isnull("legal_title_2"),
                struct(
                    lit("lgl_ttl_ln_2").alias("contractTitleLineType"),
                    col("legal_title_2").alias("contractTitleLine"),
                ).alias("contractTitle"),
            ),
        )

        contract_titles = filter(contract_title_array, lambda x: ~isnull(x))

        tax_identifier = struct(
            col("tax_id_type").alias("taxIdType"), col("tax_id").alias("taxId")
        ).alias("taxIdentifier")

        return df.select(
            "account_id",
            self._add_insert(col("account_id"), "contractIdentifier"),
            self._add_insert(col("source_sys"), "sourceSystemIdentifier"),
            self._add_insert(col("account_start_date"), "contactStartDateTime"),
            self._add_insert(contract_titles, "contractTitle"),
            self._add_insert(tax_identifier, "taxIdentifier"),
            self._add_insert(col("branch_code"), "contractBranchCode"),
            self._add_insert(col("country"), "contractCountry"),
        ).where(col("active_ind") == 1)

    def join(self, account_df: DataFrame, party_address_df: DataFrame) -> DataFrame:
        return account_df.join(party_address_df, "account_id", "left_outer")
