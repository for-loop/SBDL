import abc

from pyspark.sql.dataframe import DataFrame

from exceptions.MissingTargetLocationError import MissingTargetLocationError


class Loader(metaclass=abc.ABCMeta):

    def __init__(self, conf):
        self.__conf = conf
        self.__default_value = ""

    @abc.abstractmethod
    def load(self, df: DataFrame) -> None:
        pass

    def get_target_location(self, key):
        value = self.__conf.get(key, self.__default_value)

        if len(value) == 0:
            raise MissingTargetLocationError(f"{key} is required in sbdl.conf")

        return value
