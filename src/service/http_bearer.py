"""
Alteration of FastAPI's HTTPBearer class to handle the KBase authorization
step and allow for more informative errors.

Also adds an `optional` keyword argument that allows for missing, but not malformed,
authentication headers. If `optional` is `True` and no authorization header is provided, `None`
will be returned in place of the normal `KBaseUser`.
"""

from fastapi.security.http import HTTPBase
from fastapi.openapi.models import HTTPBearer as HTTPBearerModel
from fastapi.security.utils import get_authorization_scheme_param
from fastapi.requests import Request
from src.service import app_state
from src.service import errors
from src.service import kb_auth
from src.service.user import UserID
from typing import Optional, NamedTuple

# Modified from https://github.com/tiangolo/fastapi/blob/e13df8ee79d11ad8e338026d99b1dcdcb2261c9f/fastapi/security/http.py#L100

_SCHEME = "Bearer"


class KBaseUser(NamedTuple):
    user: UserID
    admin_perm: kb_auth.AdminPermission
    token: str


class KBaseHTTPBearer(HTTPBase):
    def __init__(
        self,
        *,
        bearerFormat: Optional[str] = None,
        scheme_name: Optional[str] = None,
        description: Optional[str] = None,
        # FastAPI uses auto_error, but that allows for malformed headers as well as just
        # no header. Use a different variable name since the behavior is different.
        optional: bool = False,
        # Considered adding a required auth role here and throwing an exception if the user
        # doesn't have it, but often you want to customize the error message.
        # Easier to handle that in the route method.
    ):
        self.model = HTTPBearerModel(bearerFormat=bearerFormat, description=description)
        self.scheme_name = scheme_name or self.__class__.__name__
        self.optional = optional

    async def __call__(self, request: Request) -> KBaseUser:
        authorization: str = request.headers.get("Authorization")
        if not authorization:
            if self.optional:
                return None
            raise errors.MissingTokenError("Authorization header required")
        scheme, credentials = get_authorization_scheme_param(authorization)
        if not (scheme and credentials):
            raise errors.InvalidAuthHeader(
                f"Authorization header requires {_SCHEME} scheme followed by token")
        if scheme.lower() != _SCHEME.lower():
            # don't put the received scheme in the error message, might be a token
            raise errors.InvalidAuthHeader(f"Authorization header requires {_SCHEME} scheme")
        try:
            admin_perm, user = await app_state.get_kbase_auth(request).get_user(credentials)
        except kb_auth.InvalidTokenError:
            raise errors.InvalidTokenError()
        return KBaseUser(user, admin_perm, credentials)
