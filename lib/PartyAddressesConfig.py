from lib.EntitiesConfig import EntitiesConfig


class PartyAddressesConfig(EntitiesConfig):

    @property
    def source_location(self):
        return self.conf.get("address.source.location", self.default_value)

    @property
    def schema(self):
        return self.conf.get("address.schema", self.default_value)
