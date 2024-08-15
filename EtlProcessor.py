from lib.Utils import get_spark_session
from lib.logger import Log4j
from conf.ConfigLoader import get_config
from conf.AccountsConfig import AccountsConfig
from conf.PartiesConfig import PartiesConfig
from conf.PartyAddressesConfig import PartyAddressesConfig
from extract.ExtractorFactoryImpl import ExtractorFactoryImpl
from transform.TransformerFacade import TransformerFacade
from load.LoaderFacade import LoaderFacade


class EtlProcessor:

    def __init__(self, args):
        self.__env, self.__load_date = args.get_all()
        self.__spark = get_spark_session(self.__env)
        self.__logger = Log4j(self.__spark)
        self.__conf = get_config(self.__env)

    def __start_process(self):
        self.__logger.info(f"Env: {self.__env}")
        self.__logger.info(f"Load Date: {self.__load_date}")
        self.__logger.info("Created Spark Session")

    def __get_entities_config(self):
        accounts_config = AccountsConfig(self.__conf)
        parties_config = PartiesConfig(self.__conf)
        party_address_config = PartyAddressesConfig(self.__conf)

        return (accounts_config, parties_config, party_address_config)

    def __extract_source_data(self):
        accounts_config, parties_config, party_address_config = (
            self.__get_entities_config()
        )

        self.__logger.info("Extracting Source data")

        ef = ExtractorFactoryImpl(self.__conf)
        e = ef.make_extractor(self.__spark)

        df_accounts = e.extract(accounts_config)
        df_parties = e.extract(parties_config)
        df_party_address = e.extract(party_address_config)

        self.__logger.info("Extracted Source data")

        return (df_accounts, df_parties, df_party_address)

    def __transform_data(self, df_accounts, df_parties, df_party_address):
        self.__logger.info("Transforming data")

        transformer = TransformerFacade()
        transformed_df = transformer.transform(
            df_accounts, df_parties, df_party_address
        )

        self.__logger.info("Transformed data")

        return transformed_df

    def __load_to_target(self, transformed_df):
        self.__logger.info("Loading data")

        lf = LoaderFacade(self.__conf)
        lf.load(transformed_df)

        self.__logger.info("Loaded data")

    def __end_process(self):
        self.__spark.stop()
        self.__logger.info("Stopped Spark Session")

    def process(self):
        self.__start_process()

        df_accounts, df_parties, df_party_address = self.__extract_source_data()

        transformed_df = self.__transform_data(
            df_accounts, df_parties, df_party_address
        )

        self.__load_to_target(transformed_df)

        self.__end_process()
