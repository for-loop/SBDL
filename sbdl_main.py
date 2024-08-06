import sys

from lib.ArgParser import ArgParser
from lib.Utils import get_spark_session
from lib.logger import Log4j
from lib.ConfigLoader import get_config
from lib.ExtractorFactoryImpl import ExtractorFactoryImpl
from lib.Extractor import Extractor

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

    df_accounts = e.extract(conf["accounts.data.location"])
    df_parties = e.extract(conf["party.data.location"])
    df_party_address = e.extract(conf["address.data.location"])

    logger.info("Extracted Source data")

    spark.stop()
    logger.info("Stopped Spark Session")
