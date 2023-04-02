"""
A minimal async client for talking to KBase SDK generated servers (for example the Workspace).
"""

import aiohttp
import random
from typing import Any


class ServerError(Exception):

    def __init__(self, name=None, code=None, message=None, data=None, error=None):
        # name and code are intentinally discarded
        super().__init__(message)
        self.message = '' if message is None else message
        self.data = data or error or ''
        # data = JSON RPC 2.0, error = 1.1

    def __str__(self):
        return f"{self.message}\n{self.data}"


class SDKAsyncClient:
    """
    An async client for KBase SDK generated servers.
    """

    def __init__(self, url: str):
        """
        Create the client.

        url - the url of the server.
        """
        # Only create 1 session per process:
        # https://docs.aiohttp.org/en/stable/client_quickstart.html#make-a-request
        self._session = aiohttp.ClientSession()
        self._url = url

    async def call(self, method: str, params: list[Any] = None, token: str = None):
        """
        Make a service call.

        method - the name of the method, for example `Workspace.ver`.
        params - the parameters for the method.
        token - the user's KBase token, if any.

        Returns will be as documented in the spec for the respective service.
        """
        body = {
            'method': method,
            'params': params or [],
            'version': '1.1',
            'id': str(random.random())[2:]
        }
        headers = {"AUTHORIZATION": token} if token else {}
        # May need an option to trust self signed certs?
        # A lot of the error conditions below are going to be annoying to test without some
        # sort of gross monkey patching
        async with self._session.post(self._url, json=body, headers=headers) as resp:
            if resp.status == 500:  # standard error code for SDK services
                if resp.content_type == "application/json":
                    err = await resp.json()
                    if "error" in err:
                        raise ServerError(**err["error"])
                    else:
                        raise ServerError(message=await resp.text())
                else:
                    raise ServerError(message=await resp.text())
            if not resp.ok:
                resp.raise_for_status()
            res = await resp.json()
            if "result" not in res:
                raise ServerError(message="An unexpected server error occurred")
            if not res["result"]:
                return
            if len(res["result"]) == 1:
                return res["result"][0]
            return res["result"]

    async def close(self):
        """
        Close the client and release resources.
        """
        await self._session.close()
