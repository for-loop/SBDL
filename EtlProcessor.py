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

    def process(self):
        self.__logger.info(f"Env: {self.__env}")
        self.__logger.info(f"Load Date: {self.__load_date}")

        self.__logger.info("Created Spark Session")

        ef = ExtractorFactoryImpl(self.__conf)
        e = ef.make_extractor(self.__spark)

        self.__logger.info("Extracting Source data")

        accounts_config = AccountsConfig(self.__conf)
        parties_config = PartiesConfig(self.__conf)
        party_address_config = PartyAddressesConfig(self.__conf)

        df_accounts = e.extract(accounts_config)
        df_parties = e.extract(parties_config)
        df_party_address = e.extract(party_address_config)

        self.__logger.info("Extracted Source data")

        self.__logger.info("Transforming data")

        transformer = TransformerFacade()
        transformed_df = transformer.transform(
            df_accounts, df_parties, df_party_address
        )

        self.__logger.info("Transformed data")

        self.__logger.info("Loading data")

        lf = LoaderFacade(self.__conf)
        lf.load(transformed_df)

        self.__logger.info("Loaded data")

        self.__spark.stop()
        self.__logger.info("Stopped Spark Session")
