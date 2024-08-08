import pytest

from lib.AccountsConfig import AccountsConfig
from lib.PartiesConfig import PartiesConfig
from lib.PartyAddressesConfig import PartyAddressesConfig

from lib.MissingSourceLocationError import MissingSourceLocationError
from lib.MissingSchemaError import MissingSchemaError


def test_accounts_config_throws_when_source_location_is_missing():
    conf = {}
    accounts_config = AccountsConfig(conf)

    with pytest.raises(MissingSourceLocationError):
        accounts_config.source_location


def test_accounts_config_throws_when_schema_is_missing():
    conf = {}
    accounts_config = AccountsConfig(conf)

    with pytest.raises(MissingSchemaError):
        accounts_config.schema


def test_parties_config_throws_when_source_location_is_missing():
    conf = {}
    parties_config = PartiesConfig(conf)

    with pytest.raises(MissingSourceLocationError):
        parties_config.source_location


def test_parties_config_throws_when_schema_is_missing():
    conf = {}
    parties_config = PartiesConfig(conf)

    with pytest.raises(MissingSchemaError):
        parties_config.schema


def test_party_addresses_config_throws_when_source_location_is_missing():
    conf = {}
    party_addresses_config = PartyAddressesConfig(conf)

    with pytest.raises(MissingSourceLocationError):
        party_addresses_config.source_location


def test_party_addresses_config_throws_when_schema_is_missing():
    conf = {}
    party_addresses_config = PartyAddressesConfig(conf)

    with pytest.raises(MissingSchemaError):
        party_addresses_config.schema


def test_accounts_config_returns_source_location_in_conf():
    expected_source_location = "source/location/"
    expected_schema = "id int,name, string"

    conf = {
        "accounts.source.location": expected_source_location,
        "accounts.schema": expected_schema,
    }
    accounts_config = AccountsConfig(conf)

    assert accounts_config.source_location == expected_source_location
    assert accounts_config.schema == expected_schema
