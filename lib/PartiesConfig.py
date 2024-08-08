from lib.EntitiesConfig import EntitiesConfig


class PartiesConfig(EntitiesConfig):

    @property
    def source_location(self):
        return self.conf["party.source.location"]

    @property
    def schema(self):
        return self.conf["party.schema"]
