from lib.EntitiesConfig import EntitiesConfig


class PartyAddressesConfig(EntitiesConfig):

    @property
    def source_location(self):
        return self._get_source_location("address.source.location")

    @property
    def schema(self):
        return self._get_schema("address.schema")
