import abc

from lib.MissingSourceLocationError import MissingSourceLocationError
from lib.MissingSchemaError import MissingSchemaError


class EntitiesConfig(metaclass=abc.ABCMeta):

    def __init__(self, conf):
        self.conf = conf
        self.default_value = ""

    @property
    @abc.abstractmethod
    def source_location(self):
        pass

    @property
    @abc.abstractmethod
    def schema(self):
        pass

    def _get_source_location(self, key: str) -> str:
        value = self.conf.get(key, self.default_value)

        if len(value) == 0:
            raise MissingSourceLocationError(f"{key} is required in sbdl.conf")

        return value

    def _get_schema(self, key: str) -> str:
        value = self.conf.get(key, self.default_value)

        if len(value) == 0:
            raise MissingSchemaError(f"{key} is required in sbdl.conf")

        return value
