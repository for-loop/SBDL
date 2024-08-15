import abc

from exceptions.MissingTargetFormatError import MissingTargetFormatError
from exceptions.UnsupportedTargetFormatError import UnsupportedTargetFormatError


class LoaderFactory(metaclass=abc.ABCMeta):

    def __init__(self, conf):
        self._conf = conf
        self.__supported_target_formats = {"json"}

    def _get_target_format(self):
        key = "target.format"
        target_format = self._conf.get(key, "")
        if len(target_format) == 0:
            raise MissingTargetFormatError(f"{key} is required in sbdl.conf.")

        if target_format not in self.__supported_target_formats:
            raise UnsupportedTargetFormatError(
                f"{target_format} is currently unsupported. The supported formats are {self.__supported_target_formats}"
            )

        return target_format

    @abc.abstractmethod
    def make_loader(self):
        pass
