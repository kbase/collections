'''
API for the collections service.
'''

import os
import sys

from fastapi import FastAPI, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from http.client import responses
from pydantic import BaseModel, Field
from starlette.exceptions import HTTPException as StarletteHTTPException

from src.common.version import VERSION
from src.service import app_state
from src.service import errors
from src.service import models_errors
from src.service.config import CollectionsServiceConfig
from src.service.routes_collections import (
    ROUTER_GENERAL,
    ROUTER_COLLECTIONS,
    ROUTER_COLLECTIONS_ADMIN,
    SERVICE_NAME
)
from src.service.timestamp import timestamp


# TODO LOGGING - log all write ops

_KB_DEPLOYMENT_CONFIG = "KB_DEPLOYMENT_CONFIG"

SERVICE_DESCRIPTION = "A repository of data collections and and associated analyses"


def create_app(noop=False):
    """
    Create the Collections application
    """
    # deliberately not documenting noop, should go away when we have real tests
    if noop:
        # temporary for prototype status. Eventually need full test suite with 
        # config file, all service dependencies, etc.
        return

    with open(os.environ[_KB_DEPLOYMENT_CONFIG], 'rb') as cfgfile:
        cfg = CollectionsServiceConfig(cfgfile)
    cfg.print_config(sys.stdout)
    sys.stdout.flush()

    app = FastAPI(
        title = SERVICE_NAME,
        description = SERVICE_DESCRIPTION,
        version = VERSION,
        root_path = cfg.service_root_path or "",
        exception_handlers = {
            errors.CollectionError: _handle_app_exception,
            RequestValidationError: _handle_fastapi_validation_exception,
            StarletteHTTPException: _handle_http_exception,
            Exception: _handle_general_exception
        },
        responses = {
            "4XX": {"model": models_errors.ClientError},
            "5XX": {"model": models_errors.ServerError}
        }
    )
    app.include_router(ROUTER_GENERAL)
    app.include_router(ROUTER_COLLECTIONS)
    # add data products routers here
    app.include_router(ROUTER_COLLECTIONS_ADMIN)

    async def build_app_wrapper():
        await app_state.build_app(app, cfg)
    app.add_event_handler("startup", build_app_wrapper)

    async def clean_app_wrapper():
        await app_state.clean_app(app)
    app.add_event_handler("shutdown", clean_app_wrapper)
    return app


def _handle_app_exception(r: Request, exc: errors.CollectionError):
    if isinstance(exc, errors.AuthenticationError):
        status_code = status.HTTP_401_UNAUTHORIZED
    elif isinstance(exc, errors.UnauthorizedError):
        status_code = status.HTTP_403_FORBIDDEN
    elif isinstance(exc, errors.NoDataException):
        status_code = status.HTTP_404_NOT_FOUND
    else:
        status_code = status.HTTP_400_BAD_REQUEST
    return _format_error(status_code, exc.message, exc.error_type)
    

def _handle_fastapi_validation_exception(r: Request, exc: RequestValidationError):
    return _format_error(
        status.HTTP_400_BAD_REQUEST,
        error_type=errors.ErrorType.REQUEST_VALIDATION_FAILED,
        request_validation_detail=exc.errors()
    )

def _handle_http_exception(r: Request, exc: StarletteHTTPException):
    # may need to expand this in the future, mainly handles 404s
    return _format_error(exc.status_code, message=str(exc.detail))


def _handle_general_exception(r: Request, exc: Exception):
    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    if len(exc.args) == 1 and type(exc.args[0]) == str:
        return _format_error(status_code, exc.args[0])
    else:
        return _format_error(status_code)
        

def _format_error(
        status_code: int,
        message: str = None,
        error_type: errors.ErrorType = None,
        request_validation_detail = None
        ):
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



# TODO REFACTOR auth handling. Calling check_admin anyway, might was well do it all there
# rather than with an overly complex Depends... but that's needed for OpenAPI to know about
# the header. hmm. Also may need to be optional for some cases, now it's all or nothing
# https://stackoverflow.com/questions/70926257/how-to-pass-authorization-header-from-swagger-doc-in-python-fast-api
# TODO REFACTOR move routes into separate modules, routes and routes_common for things
# that data products may need
# TODO STRUCTURE figure out how to structure data product code, ideally shoudln't touch main code
