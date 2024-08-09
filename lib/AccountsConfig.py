from lib.EntitiesConfig import EntitiesConfig


class AccountsConfig(EntitiesConfig):

    @property
    def source_location(self):
        return self._get_source_location("accounts.source.location")

    @property
    def schema(self):
        return self._get_schema("accounts.schema")
