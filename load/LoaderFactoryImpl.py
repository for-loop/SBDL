from load.LoaderFactory import LoaderFactory
from load.JsonLoader import JsonLoader

from exceptions.UnsupportedTargetFormatError import UnsupportedTargetFormatError


class LoaderFactoryImpl(LoaderFactory):

    def make_loader(self):
        if self._get_target_format() == "json":
            return JsonLoader(self._conf)

        raise UnsupportedTargetFormatError(f"{target_format} is currently unsupported.")
