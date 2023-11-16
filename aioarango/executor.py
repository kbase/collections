from typing import Callable, TypeVar, Union

from aioarango.connection import Connection
from aioarango.request import Request
from aioarango.response import Response

ApiExecutor = Union[
    "DefaultApiExecutor",
    "AsyncApiExecutor",
    "BatchApiExecutor",
    "TransactionApiExecutor",
]

T = TypeVar("T")


class DefaultApiExecutor:
    """Default API executor.

    :param connection: HTTP connection.
    :type connection: aioarango.connection.BasicConnection |
        aioarango.connection.JwtConnection | aioarango.connection.JwtSuperuserConnection
    """

    def __init__(self, connection: Connection) -> None:
        self._conn = connection

    @property
    def context(self) -> str:
        return "default"

    async def execute(self, request: Request, response_handler: Callable[[Response], T]) -> T:
        """Execute an API request and return the result.

        :param request: HTTP request.
        :type request: aioarango.request.Request
        :param response_handler: HTTP response handler.
        :type response_handler: callable
        :return: API execution result.
        """
        resp = await self._conn.send_request(request)
        return response_handler(resp)
