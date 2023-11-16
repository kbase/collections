from abc import abstractmethod
from typing import Any, Callable, Optional, Sequence, Union

import httpx
from requests_toolbelt import MultipartEncoder

from aioarango.exceptions import ServerConnectionError
from aioarango.http import HTTPClient
from aioarango.request import Request
from aioarango.resolver import HostResolver
from aioarango.response import Response
from aioarango.typings import Fields, Json

Connection = Union['BaseConnection']

class BaseConnection(object):
    """Base connection to a specific ArangoDB database."""

    def __init__(
        self,
        hosts: Fields,
        host_resolver: HostResolver,
        sessions: Sequence[httpx.AsyncClient],
        db_name: str,
        http_client: HTTPClient,
        serializer: Callable[..., str],
        deserializer: Callable[[str], Any],
    ):
        self._url_prefixes = [f"{host}/_db/{db_name}" for host in hosts]
        self._host_resolver = host_resolver
        self._sessions = sessions
        self._db_name = db_name
        self._http = http_client
        self._serializer = serializer
        self._deserializer = deserializer
        self._username: Optional[str] = None

    @property
    def db_name(self) -> str:
        """Return the database name.

        :returns: Database name.
        :rtype: str
        """
        return self._db_name

    @property
    def username(self) -> Optional[str]:
        """Return the username.

        :returns: Username.
        :rtype: str
        """
        return self._username

    def serialize(self, obj: Any) -> str:
        """Serialize the given object.

        :param obj: JSON object to serialize.
        :type obj: str | bool | int | float | list | dict | None
        :return: Serialized string.
        :rtype: str
        """
        return self._serializer(obj)

    def deserialize(self, string: str) -> Any:
        """De-serialize the string and return the object.

        :param string: String to de-serialize.
        :type string: str
        :return: De-serialized JSON object.
        :rtype: str | bool | int | float | list | dict | None
        """
        try:
            return self._deserializer(string)
        except (ValueError, TypeError):
            return string

    def prep_response(self, resp: Response, deserialize: bool = True) -> Response:
        """Populate the response with details and return it.

        :param deserialize: Deserialize the response body.
        :type deserialize: bool
        :param resp: HTTP response.
        :type resp: aioarango.response.Response
        :return: HTTP response.
        :rtype: aioarango.response.Response
        """
        if deserialize:
            resp.body = self.deserialize(resp.raw_body)
            if isinstance(resp.body, dict):
                resp.error_code = resp.body.get("errorNum")
                resp.error_message = resp.body.get("errorMessage")
        else:
            resp.body = resp.raw_body

        http_ok = 200 <= resp.status_code < 300
        resp.is_success = http_ok and resp.error_code is None
        return resp

    def prep_bulk_err_response(self, parent_response: Response, body: Json) -> Response:
        """Build and return a bulk error response.

        :param parent_response: Parent response.
        :type parent_response: aioarango.response.Response
        :param body: Error response body.
        :type body: dict
        :return: Child bulk error response.
        :rtype: aioarango.response.Response
        """
        resp = Response(
            method=parent_response.method,
            url=parent_response.url,
            headers=parent_response.headers,
            status_code=parent_response.status_code,
            status_text=parent_response.status_text,
            raw_body=self.serialize(body),
        )
        resp.body = body
        resp.error_code = body["errorNum"]
        resp.error_message = body["errorMessage"]
        resp.is_success = False
        return resp

    def normalize_data(self, data: Any) -> Union[str, MultipartEncoder, None]:
        """Normalize request data.

        :param data: Request data.
        :type data: str | MultipartEncoder | None
        :return: Normalized data.
        :rtype: str | MultipartEncoder | None
        """
        if data is None:
            return None
        elif isinstance(data, str):
            return data
        elif isinstance(data, MultipartEncoder):
            return data.read()
        else:
            return self.serialize(data)

    async def ping(self) -> int:
        """Ping the next host to check if connection is established.

        :return: Response status code.
        :rtype: int
        """
        request = Request(method="get", endpoint="/_api/collection")
        resp = await self.send_request(request)
        if resp.status_code in {401, 403}:
            raise ServerConnectionError("bad username and/or password")
        if not resp.is_success:  # pragma: no cover
            raise ServerConnectionError(resp.error_message or "bad server response")
        return resp.status_code

    @abstractmethod
    async def send_request(self, request: Request) -> Response:  # pragma: no cover
        """Send an HTTP request to ArangoDB server.

        :param request: HTTP request.
        :type request: aioarango.request.Request
        :return: HTTP response.
        :rtype: aioarango.response.Response
        """
        raise NotImplementedError


class BasicConnection(BaseConnection):
    """Connection to specific ArangoDB database using basic authentication.

    :param hosts: Host URL or list of URLs (coordinators in a cluster).
    :type hosts: [str]
    :param host_resolver: Host resolver (used for clusters).
    :type host_resolver: aioarango.resolver.HostResolver
    :param sessions: HTTP session objects per host.
    :type sessions: [requests.Session]
    :param db_name: Database name.
    :type db_name: str
    :param username: Username.
    :type username: str
    :param password: Password.
    :type password: str
    :param http_client: User-defined HTTP client.
    :type http_client: aioarango.http.HTTPClient
    """

    def __init__(
        self,
        hosts: Fields,
        host_resolver: HostResolver,
        sessions: Sequence[httpx.AsyncClient],
        db_name: str,
        username: str,
        password: str,
        http_client: HTTPClient,
        serializer: Callable[..., str],
        deserializer: Callable[[str], Any],
    ) -> None:
        super().__init__(
            hosts,
            host_resolver,
            sessions,
            db_name,
            http_client,
            serializer,
            deserializer,
        )
        self._username = username
        self._auth = (username, password)

    async def send_request(self, request: Request) -> Response:
        """Send an HTTP request to ArangoDB server.

        :param request: HTTP request.
        :type request: aioarango.request.Request
        :return: HTTP response.
        :rtype: aioarango.response.Response
        """
        host_index = self._host_resolver.get_host_index()
        resp = await self._http.send_request(
            session=self._sessions[host_index],
            method=request.method,
            url=self._url_prefixes[host_index] + request.endpoint,
            params=request.params,
            data=self.normalize_data(request.data),
            headers=request.headers,
            auth=self._auth,
        )
        return self.prep_response(resp, request.deserialize)
