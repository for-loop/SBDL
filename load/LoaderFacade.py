from pyspark.sql.dataframe import DataFrame

from load.LoaderFactoryImpl import LoaderFactoryImpl


class LoaderFacade:

    def __init__(self, conf):
        self.__loader_factory_impl = LoaderFactoryImpl(conf)

    def load(self, df: DataFrame) -> None:
        loader = self.__loader_factory_impl.make_loader()
        loader.load(df)
