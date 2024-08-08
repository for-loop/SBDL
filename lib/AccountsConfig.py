from lib.EntitiesConfig import EntitiesConfig


class AccountsConfig(EntitiesConfig):

    @property
    def source_location(self):
        return self.conf["accounts.source.location"]

    @property
    def schema(self):
        return self.conf["accounts.schema"]
