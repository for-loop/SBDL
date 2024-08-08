import pytest

from lib.AccountsConfig import AccountsConfig
from lib.PartiesConfig import PartiesConfig
from lib.PartyAddressesConfig import PartyAddressesConfig


def test_accounts_config_returns_empty_string_when_source_location_or_schema_is_missing():
    conf = {}
    accounts_config = AccountsConfig(conf)

    assert accounts_config.source_location == accounts_config.default_value
    assert accounts_config.schema == accounts_config.default_value


def test_parties_config_returns_empty_string_when_source_location_or_schema_is_missing():
    conf = {}
    parties_config = PartiesConfig(conf)

    assert parties_config.source_location == parties_config.default_value
    assert parties_config.schema == parties_config.default_value


def test_party_addresses_config_returns_empty_string_when_source_location_or_schema_is_missing():
    conf = {}
    party_addresses_config = PartyAddressesConfig(conf)

    assert (
        party_addresses_config.source_location == party_addresses_config.default_value
    )
    assert party_addresses_config.schema == party_addresses_config.default_value


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
