from json import dumps, loads
from typing import Sequence, Union

from aioarango.connection import BasicConnection
from aioarango.database import StandardDatabase
from aioarango.exceptions import ServerConnectionError
from aioarango.http import DefaultHTTPClient
from aioarango.resolver import (
    RoundRobinHostResolver,
    SingleHostResolver,
)
from aioarango.version import __version__


class ArangoClient:
    """ArangoDB client.

    :param hosts: Host URL or list of URLs (coordinators in a cluster).
    :type hosts: str | [str]
    """

    def __init__(
        self,
        hosts: Union[str, Sequence[str]] = "http://127.0.0.1:8529",
    ) -> None:
        if isinstance(hosts, str):
            self._hosts = [host.strip("/") for host in hosts.split(",")]
        else:
            self._hosts = [host.strip("/") for host in hosts]

        host_count = len(self._hosts)

        if host_count == 1:
            self._host_resolver = SingleHostResolver()
        else:
            self._host_resolver = RoundRobinHostResolver(host_count)

        self._http = DefaultHTTPClient()
        self._serializer = dumps
        self._deserializer = loads
        self._sessions = [self._http.create_session(h) for h in self._hosts]

    def __repr__(self) -> str:
        return f"<ArangoClient {','.join(self._hosts)}>"

    async def close(self):
        for session in self._sessions:
            await session.aclose()

    @property
    def hosts(self) -> Sequence[str]:
        """Return the list of ArangoDB host URLs.

        :return: List of ArangoDB host URLs.
        :rtype: [str]
        """
        return self._hosts

    @property
    def version(self):
        """Return the client version.

        :return: Client version.
        :rtype: str
        """
        return __version__

    async def db(
        self,
        name: str = "_system",
        username: str = "root",
        password: str = "",
        verify: bool = False,
    ) -> StandardDatabase:
        """Connect to an ArangoDB database and return the database API wrapper.

        :param name: Database name.
        :type name: str
        :param username: Username for basic authentication.
        :type username: str
        :param password: Password for basic authentication.
        :type password: str
        :param verify: Verify the connection by sending a test request.
        :type verify: bool
        :return: Standard database API wrapper.
        :rtype: aioarango.database.StandardDatabase
        :raise aioarango.exceptions.ServerConnectionError: If **verify** was set
            to True and the connection fails.
        """

        connection = BasicConnection(
                hosts=self._hosts,
                host_resolver=self._host_resolver,
                sessions=self._sessions,
                db_name=name,
                username=username,
                password=password,
                http_client=self._http,
                serializer=self._serializer,
                deserializer=self._deserializer,
            )

        if verify:
            try:
                await connection.ping()
            except ServerConnectionError as err:
                raise err
            except Exception as err:
                raise ServerConnectionError(f"bad connection: {err}")

        return StandardDatabase(connection)
