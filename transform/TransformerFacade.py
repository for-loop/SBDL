from pyspark.sql.dataframe import DataFrame

from transform.AccountsTransformer import AccountsTransformer
from transform.PartiesTransformer import PartiesTransformer
from transform.AddressesTransformer import AddressesTransformer
from transform.HeadersTransformer import HeadersTransformer


class TransformerFacade:

    def __init__(self):
        self.__accounts_transformer = AccountsTransformer()
        self.__parties_transformer = PartiesTransformer()
        self.__addresses_transformer = AddressesTransformer()
        self.__headers_transformer = HeadersTransformer()

    def transform(
        self, accounts_df: DataFrame, parties_df: DataFrame, addresses_df: DataFrame
    ) -> DataFrame:

        transformed_parties_df = self.__parties_transformer.transform(parties_df)
        transformed_addresses_df = self.__addresses_transformer.transform(addresses_df)

        joined_party_addresses_df = self.__parties_transformer.join(
            transformed_parties_df, transformed_addresses_df
        )

        transformed_accounts_df = self.__accounts_transformer.transform(accounts_df)
        joined_account_party_addresses_df = self.__accounts_transformer.join(
            transformed_accounts_df, joined_party_addresses_df
        )

        return self.__headers_transformer.transform(joined_account_party_addresses_df)
