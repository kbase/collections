"""
A configuration parser for the collections service. The configuration is expected to be in TOML
(https://toml.io/en/) format.
"""

import tomli  # TODO swap to stdlib in py 3.11
from typing import Optional, BinaryIO, TextIO

_SEC_ARANGO = "Arango"
_SEC_AUTH = "Authentication"
_SEC_SERVICE = "Service"


class CollectionsServiceConfig:
    """
    The collections service configuration parsed from a TOML configuration file. Once initialized,
    this class will contain the fields:

    arango_url: str - the URL of an arango coordinator.
    arango_db: str - the name of the ArangoDB database to update.
    arango_user: str | None - the name, if any, of the user to use when connecting to
        ArangoDB.
    arango_pwd: str | None - the password for the user. Present IFF the user is present.
    
    auth_url: str - the URL of the KBase Auth2 service.
    auth_full_admin_roles: list[str] - the list of Auth2 custom roles that signify that a user is
        a full admin for the collections service
    
    service_root_path: str  | None - if the service is behind a reverse proxy that rewrites the
        service path, the path to the service. The path is required in order for the OpenAPI
        documentation to function.
    create_db_on_startup: bool - True if the service should create the database on startup.
        Generally this should be false to allow admins to set up sharding as desired.
    dont_connect_to_external_services: bool - True if the service should not connect to
        any external services except for authorization, including the database. This
        will cause all calls to the service that require external service access to fail but is
        useful for quickly checking OpenAPI documentation or general methods that don't
        access the external services.
    """

    def __init__(self, config_file: BinaryIO):
        """
        Create the configuration parser.
        config_file - an open file-like object, opened in binary mode, containing the TOML
            config file data.
        """
        if not config_file:
            raise ValueError("config_file is required")
        # Since this is service startup and presumably the person starting the server is
        # savvy enough to figure out toml errors, we just throw the errors as is
        config = tomli.load(config_file)
        # I feel like there ought to be a lib to do this kind of stuff... jsonschema doesn't
        # quite do what I want
        _check_missing_section(config, _SEC_ARANGO)
        _check_missing_section(config, _SEC_AUTH)
        _check_missing_section(config, _SEC_SERVICE)
        self.arango_url = _get_string_required(config, _SEC_ARANGO, "url")
        self.arango_db = _get_string_required(config, _SEC_ARANGO, "database")
        self.arango_user = _get_string_optional(config, _SEC_ARANGO, "username")
        self.arango_pwd = _get_string_optional(config, _SEC_ARANGO, "password")
        if self.arango_user and not self.arango_pwd:
            raise ValueError(
                f"If username is present in the {_SEC_ARANGO} section, password must be present")

        self.auth_url = _get_string_required(config, _SEC_AUTH, "url")
        self.auth_full_admin_roles = _get_list_string(config, _SEC_AUTH, "admin_roles_full")

        self.service_root_path = _get_string_optional(config, _SEC_SERVICE, "root_path")
        self.create_db_on_startup = _get_string_optional(
            config, _SEC_SERVICE, "create_db_on_startup") == "true"
        self.dont_connect_to_external_services = _get_string_optional(
            config, _SEC_SERVICE, "dont_connect_to_external_services") == "true"

    def print_config(self, output: TextIO):
        """
        Print the configuration to the output argument, censoring secrets.
        """
        output.writelines([
            "\n*** Service Configuration ***\n",
            f"Arango URL: {self.arango_url}\n",
            f"Arango database: {self.arango_db}\n",
        ])
        if self.arango_user:
            output.writelines([
                f"Arango username: {self.arango_user}\n",
                f"Arango password: [REDACTED FOR YOUR SAFETY AND ENJOYMENT]\n",
            ])
        output.writelines([
            f"Authentication URL: {self.auth_url}\n",
            f"Authentication full admin roles: {self.auth_full_admin_roles}\n",
            f"Service root path: {self.service_root_path}\n",
            f"Create database on start: {self.create_db_on_startup}\n"
            f"Don't connect to external services: "
                + f"{self.dont_connect_to_external_services}\n"
            "*** End Service Configuration ***\n\n"
        ])

def _check_missing_section(config, section):
    if section not in config:
        raise ValueError(f"Missing section {section}")


# assumes section exists
def _get_string_required(config, section, key) -> str:
    putative = _get_string_optional(config, section, key)
    if not putative:
        raise ValueError(f"Missing value for key {key} in section {section}")
    return putative


# assumes section exists
def _get_string_optional(config, section, key) -> Optional[str]:
    putative = config[section].get(key)
    if putative is None:
        return None
    if type(putative) != str:
        raise ValueError(
            f"Expected string value for key {key} in section {section}, got {putative}")
    if not putative.strip():
        return None
    return putative.strip()


#assumes section exists
def _get_list_string(config, section, key) -> list[str]:
    putative = _get_string_optional(config, section, key)
    if not putative:
        return []
    return [x.strip() for x in putative.split(",")]