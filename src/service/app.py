'''
API for the collections service.
'''
import os

from src.common.git_commit import GIT_COMMIT
from src.common.version import VERSION
from datetime import datetime, timezone
from fastapi import FastAPI, Depends, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError, HTTPException
from fastapi.responses import JSONResponse
from fastapi.security.http import HTTPBearer, HTTPBasicCredentials
from http.client import responses
from pydantic import BaseModel, Field

from src.service import kb_auth
from src.service import errors


# TODO LOGGING - log all write ops


# TODO CONFIG these should be configured vs hard coded
AUTH2_ADMIN_ROLES = ["KBASE_ADMIN", "COLLECTIONS_SERVICE_ADMIN"]
CI_AUTH_URL = "https://ci.kbase.us/services/auth"

SERVICE_NAME = "Collections Prototype"

app_config = {
    "title": SERVICE_NAME,
    "description": "A repository of data collections and and associated analyses",
    "version": VERSION,
}


# TODO CONFIG rethink how to do this. Config file? How to test? Maybe manually is the only
# reasonable option
_FASTAPI_ROOT_PATH = "FASTAPI_ROOT_PATH"
if os.environ.get(_FASTAPI_ROOT_PATH):
    app_config.update({
        "root_path": os.environ[_FASTAPI_ROOT_PATH],
    })


app = FastAPI(**app_config)
authheader = HTTPBearer()


@app.on_event('startup')
async def build():
    app.state.kb_auth = await kb_auth.create_auth_client(CI_AUTH_URL, AUTH2_ADMIN_ROLES)


@app.exception_handler(errors.CollectionError)
async def handle_app_exception(r: Request, exc: errors.CollectionError):
    if isinstance(exc, errors.InvalidTokenError):
        status_code = status.HTTP_401_UNAUTHORIZED
    elif isinstance(exc, errors.UnauthorizedError):
        status_code = status.HTTP_403_FORBIDDEN
    elif isinstance(exc, errors.NoDataException):
        status_code = status.HTTP_404_NOT_FOUND
    else:
        status_code = status.HTTP_400_BAD_REQUEST
    return format_error(status_code, exc.message, exc.error_type)
    

@app.exception_handler(RequestValidationError)
async def handle_fastapi_validation_exception(r: Request, exc: RequestValidationError):
    return format_error(
        status.HTTP_400_BAD_REQUEST,
        error_type=errors.REQUEST_VALIDATION_FAILED,
        request_validation_detail=exc.detail()
    )


@app.exception_handler(Exception)
async def handle_general_exception(r: Request, exc: Exception):
    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    if len(exc.args) == 1 and type(exc.args[0]) == str:
        return format_error(status_code, exc.args[0])
    else:
        return format_error(status_code)
        

def format_error(
        status_code: int,
        message: str = None,
        error_type: errors.ErrorType = None,
        request_validation_detail = None
        ):
    # TODO DOCS document error structure
    content = {"httpcode": status_code, "httpstatus": responses[status_code]}
    if error_type:
        content.update({
            "appcode": error_type.error_code, "apperror": error_type.error_type})
    if message:
        content.update({"message": message})
    if request_validation_detail:
        content.update({"request_validation_detail": request_validation_detail})
    return JSONResponse(status_code=status_code, content=jsonable_encoder(content))


async def get_user(creds:HTTPBasicCredentials):
    try:
        return await app.state.kb_auth.get_user(creds.credentials)
    except kb_auth.InvalidTokenError:
        raise errors.InvalidTokenError()


def timestamp():
    return datetime.now(timezone.utc).isoformat()


class Root(BaseModel):
    service_name: str = Field(example=SERVICE_NAME)
    version: str = Field(example=VERSION)
    git_hash: str = Field(example="b78f6e15e85381a7df71d6005d99e866f3f868dc")
    server_time: str = Field(example="2022-10-07T17:58:53.188698+00:00")


class WhoAmI(BaseModel):
    user: str = Field(example="kbasehelp")
    is_service_admin: bool = Field(example=False)


@app.get("/", response_model=Root)
async def root():
    return {
        "service_name": SERVICE_NAME,
        "version": VERSION,
        "git_hash": GIT_COMMIT,
        "server_time": timestamp()
    }


@app.get("/whoami", response_model = WhoAmI)
async def whoami(creds: HTTPBasicCredentials=Depends(authheader)):
    admin, user = await get_user(creds)
    return {"user": user.id, "is_service_admin": kb_auth.AdminPermission.FULL == admin}

