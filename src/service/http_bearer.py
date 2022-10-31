"""
Alteration of FastAPI's HTTPBearer class to handle the KBase authorization
step and allow for more informative errors
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

# Also, for some reason an exception handler for "Exception" wasn't trapping HTTPException classes
# Modified from https://github.com/tiangolo/fastapi/blob/e13df8ee79d11ad8e338026d99b1dcdcb2261c9f/fastapi/security/http.py#L100

_SCHEME = "Bearer"


class KBaseUser(NamedTuple):
    user: UserID
    admin_perm: kb_auth.AdminPermission


class KBaseHTTPBearer(HTTPBase):
    def __init__(
        self,
        *,
        bearerFormat: Optional[str] = None,
        scheme_name: Optional[str] = None,
        description: Optional[str] = None,
    ):
        self.model = HTTPBearerModel(bearerFormat=bearerFormat, description=description)
        self.scheme_name = scheme_name or self.__class__.__name__

    async def __call__(self, request: Request) -> KBaseUser:
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
        try:
            admin_perm, user = await app_state.get_kbase_auth(request).get_user(credentials)
        except kb_auth.InvalidTokenError:
            raise errors.InvalidTokenError()
        return KBaseUser(user, admin_perm)