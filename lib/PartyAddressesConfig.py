from lib.EntitiesConfig import EntitiesConfig


class PartyAddressesConfig(EntitiesConfig):

    @property
    def source_location(self):
        return self.conf["address.source.location"]

    @property
    def schema(self):
        return self.conf["address.schema"]
