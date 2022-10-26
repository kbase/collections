'''
API for the collections service.
'''

import os
import sys

from datetime import datetime, timezone
from fastapi import FastAPI, Depends, Request, status, APIRouter
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError, HTTPException
from fastapi.responses import JSONResponse
from http.client import responses
from pydantic import BaseModel, Field

from src.common.git_commit import GIT_COMMIT
from src.common.version import VERSION
from src.service import kb_auth
from src.service import errors
from src.service.config import CollectionsServiceConfig
from src.service.http_bearer import KBaseHTTPBearer, KBaseUser


# TODO LOGGING - log all write ops

_KB_DEPLOYMENT_CONFIG = "KB_DEPLOYMENT_CONFIG"

SERVICE_NAME = "Collections Prototype"
SERVICE_DESCRIPTION = "A repository of data collections and and associated analyses"


def create_app(noop=False):
    if noop:
        # temporary for prototype status. Eventually need full test suite with 
        # config file, all service dependencies, etc.
        return

    with open(os.environ[_KB_DEPLOYMENT_CONFIG], 'rb') as cfgfile:
        cfg = CollectionsServiceConfig(cfgfile)
    cfg.print_config(sys.stdout)

    app = FastAPI(
        title = SERVICE_NAME,
        description = SERVICE_DESCRIPTION,
        version = VERSION,
        root_path = cfg.service_root_path or "",
        exception_handlers = {
            errors.CollectionError: handle_app_exception,
            RequestValidationError: handle_fastapi_validation_exception,
            Exception: handle_general_exception
        }
    )
    app.include_router(router)
    async def build_app_wrapper():
        await build_app(app, cfg)

    app.add_event_handler("startup", build_app_wrapper)
    return app


async def build_app(app, cfg):
    app.state.kb_auth = await kb_auth.create_auth_client(cfg.auth_url, cfg.auth_full_admin_roles)


router = APIRouter()
authheader = KBaseHTTPBearer()


def handle_app_exception(r: Request, exc: errors.CollectionError):
    if isinstance(exc, errors.AuthenticationError):
        status_code = status.HTTP_401_UNAUTHORIZED
    elif isinstance(exc, errors.UnauthorizedError):
        status_code = status.HTTP_403_FORBIDDEN
    elif isinstance(exc, errors.NoDataException):
        status_code = status.HTTP_404_NOT_FOUND
    else:
        status_code = status.HTTP_400_BAD_REQUEST
    return format_error(status_code, exc.message, exc.error_type)
    

def handle_fastapi_validation_exception(r: Request, exc: RequestValidationError):
    return format_error(
        status.HTTP_400_BAD_REQUEST,
        error_type=errors.REQUEST_VALIDATION_FAILED,
        request_validation_detail=exc.detail()
    )


def handle_general_exception(r: Request, exc: Exception):
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
    # TODO DOCS document error structure, see https://fastapi.tiangolo.com/advanced/additional-responses/
    # Will need to do this for each endpoint unfortunately
    # Also see https://github.com/tiangolo/fastapi/issues/1376
    content = {
        "httpcode": status_code,
        "httpstatus": responses[status_code],
        "time": timestamp()
    }
    if error_type:
        content.update({
            "appcode": error_type.error_code, "apperror": error_type.error_type})
    if message:
        content.update({"message": message})
    if request_validation_detail:
        content.update({"request_validation_detail": request_validation_detail})
    return JSONResponse(status_code=status_code, content=jsonable_encoder({"error": content}))


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


@router.get("/", response_model=Root)
async def root():
    return {
        "service_name": SERVICE_NAME,
        "version": VERSION,
        "git_hash": GIT_COMMIT,
        "server_time": timestamp()
    }


@router.get("/whoami", response_model = WhoAmI)
async def whoami(user: KBaseUser=Depends(authheader)):
    return {
        "user": user.user.id,
        "is_service_admin": kb_auth.AdminPermission.FULL == user.admin_perm
    }

