class AccountsConfig:

    def __init__(self, conf):
        self.__source_location = conf["accounts.source.location"]
        self.__schema = conf["accounts.schema"]

    @property
    def source_location(self):
        return self.__source_location

    @property
    def schema(self):
        return self.__schema
