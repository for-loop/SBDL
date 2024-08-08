import sys

from lib.ArgParser import ArgParser
from lib.Utils import get_spark_session
from lib.logger import Log4j
from lib.ConfigLoader import get_config
from lib.ExtractorFactoryImpl import ExtractorFactoryImpl
from lib.Extractor import Extractor
from lib.AccountsConfig import AccountsConfig
from lib.PartiesConfig import PartiesConfig
from lib.PartyAddressesConfig import PartyAddressesConfig

if __name__ == "__main__":

    args = ArgParser("SBDL")
    env, load_date = args.get_all()

    spark = get_spark_session(env)
    logger = Log4j(spark)

    logger.info(f"Env: {env}")
    logger.info(f"Load Date: {load_date}")

    logger.info("Created Spark Session")

    conf = get_config(env)
    ef = ExtractorFactoryImpl(conf)
    e = ef.make_extractor(spark)

    logger.info("Extracting Source data")

    accounts_config = AccountsConfig(conf)
    parties_config = PartiesConfig(conf)
    party_address_config = PartyAddressesConfig(conf)

    df_accounts = e.extract(accounts_config)
    df_parties = e.extract(parties_config)
    df_party_address = e.extract(party_address_config)

    logger.info("Extracted Source data")

    spark.stop()
    logger.info("Stopped Spark Session")
