from conf.EntitiesConfig import EntitiesConfig


class PartiesConfig(EntitiesConfig):

    @property
    def source_location(self):
        return self._get_source_location("party.source.location")

    @property
    def schema(self):
        return self._get_schema("party.schema")
