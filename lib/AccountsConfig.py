from lib.EntitiesConfig import EntitiesConfig


class AccountsConfig(EntitiesConfig):

    @property
    def source_location(self):
        return self.conf.get("accounts.source.location", self.default_value)

    @property
    def schema(self):
        return self.conf.get("accounts.schema", self.default_value)
