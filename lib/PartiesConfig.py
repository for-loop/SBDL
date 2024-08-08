class PartiesConfig:

    def __init__(self, conf):
        self.__source_location = conf["party.source.location"]
        self.__schema = conf["party.schema"]

    @property
    def source_location(self):
        return self.__source_location

    @property
    def schema(self):
        return self.__schema
