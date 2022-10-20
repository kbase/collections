"""
Override of FastAPI's HTTPBearer class to allow for more informative errors
"""

from fastapi.security.http import HTTPBase, HTTPAuthorizationCredentials
from fastapi.openapi.models import HTTPBearer as HTTPBearerModel
from fastapi.security.utils import get_authorization_scheme_param
from fastapi.requests import Request
from typing import Optional
from src.service import errors

# Also, for some reason an exception handler for "Exception" wasn't trapping HTTPException classes
# Modified from https://github.com/tiangolo/fastapi/blob/e13df8ee79d11ad8e338026d99b1dcdcb2261c9f/fastapi/security/http.py#L100

_SCHEME = "Bearer"

class HTTPBearer(HTTPBase):
    def __init__(
        self,
        *,
        bearerFormat: Optional[str] = None,
        scheme_name: Optional[str] = None,
        description: Optional[str] = None,
    ):
        self.model = HTTPBearerModel(bearerFormat=bearerFormat, description=description)
        self.scheme_name = scheme_name or self.__class__.__name__

    async def __call__(self, request: Request) -> HTTPAuthorizationCredentials:
        authorization: str = request.headers.get("Authorization")
        if not authorization:
            raise errors.MissingTokenError("Authorization header required")
        scheme, credentials = get_authorization_scheme_param(authorization)
        if not (scheme and credentials):
            raise errors.InvalidAuthHeader(
                f"Authorization header requires {_SCHEME} scheme followed by token")
        if scheme.lower() != _SCHEME.lower():
            # don't put the received scheme in the error message, might be a token
            raise errors.InvalidAuthHeader(f"Authorization header requires {_SCHEME} scheme")
        return HTTPAuthorizationCredentials(scheme=scheme, credentials=credentials)