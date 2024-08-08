from lib.EntitiesConfig import EntitiesConfig


class PartiesConfig(EntitiesConfig):

    @property
    def source_location(self):
        return self.conf.get("party.source.location", self.default_value)

    @property
    def schema(self):
        return self.conf.get("party.schema", self.default_value)
