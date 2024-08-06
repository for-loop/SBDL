import sys

from lib import Utils
from lib.logger import Log4j
from lib.ConfigLoader import get_config
from lib.ExtractorFactoryImpl import ExtractorFactoryImpl
from lib.Extractor import Extractor

if __name__ == "__main__":

    if len(sys.argv) < 3:
        print("Usage: sbdl {local, qa, prod} {load_date} : Arguments are missing")
        sys.exit(-1)

    job_run_env = sys.argv[1].upper()
    load_date = sys.argv[2]

    spark = Utils.get_spark_session(job_run_env)
    logger = Log4j(spark)

    logger.info("Created Spark Session")

    conf = get_config(job_run_env)
    ef = ExtractorFactoryImpl(conf)
    e = ef.make_extractor(spark)

    logger.info("Extracting Source data")

    df_accounts = e.extract(conf["accounts.data.location"])
    df_parties = e.extract(conf["party.data.location"])
    df_party_address = e.extract(conf["address.data.location"])

    logger.info("Extracted Source data")

    spark.stop()
    logger.info("Stopped Spark Session")
