'''
API for the collections service.
'''

import aioarango
import os
import sys
import time

from datetime import datetime, timezone
from fastapi import FastAPI, Depends, Request, status, APIRouter, Path
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError, HTTPException
from fastapi.responses import JSONResponse
from http.client import responses
from pydantic import BaseModel, Field

from src.common.git_commit import GIT_COMMIT
from src.common.version import VERSION
from src.service import kb_auth
from src.service import errors
from src.service import models
from src.service.arg_checkers import contains_control_characters
from src.service.config import CollectionsServiceConfig
from src.service.http_bearer import KBaseHTTPBearer, KBaseUser
from src.service.storage_arango import create_storage


# TODO LOGGING - log all write ops

_KB_DEPLOYMENT_CONFIG = "KB_DEPLOYMENT_CONFIG"

SERVICE_NAME = "Collections Prototype"
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

    app = FastAPI(
        title = SERVICE_NAME,
        description = SERVICE_DESCRIPTION,
        version = VERSION,
        root_path = cfg.service_root_path or "",
        exception_handlers = {
            errors.CollectionError: _handle_app_exception,
            RequestValidationError: _handle_fastapi_validation_exception,
            Exception: _handle_general_exception
        }
    )
    app.include_router(_router)
    async def build_app_wrapper():
        await _build_app(app, cfg)

    app.add_event_handler("startup", build_app_wrapper)
    return app


async def _build_app(app, cfg):
    app.state.kb_auth = await kb_auth.create_auth_client(cfg.auth_url, cfg.auth_full_admin_roles)
    cli = aioarango.ArangoClient(hosts=cfg.arango_url)
    if cfg.create_db_on_startup:
        sysdb = await _get_arango_db(cli, "_system", cfg)
        try:
            await sysdb.create_database(cfg.arango_db)
        except aioarango.exceptions.DatabaseCreateError as e:
            if e.error_code != 1207:  # duplicate name error, ignore, db exists
                raise
    db = await _get_arango_db(cli, cfg.arango_db, cfg)
    app.state.storage = await create_storage(
        db, create_collections_on_startup=cfg.create_db_on_startup)


_BACKOFF = [0, 1, 2, 5, 10, 30]


async def _get_arango_db(cli: aioarango.ArangoClient, db: str, cfg: CollectionsServiceConfig):
    for t in _BACKOFF:
        if t > 0:
            print(f"Waiting for {t}s and retrying db connection")
            time.sleep(t)
        try:
            if cfg.arango_user:
                rdb = await cli.db(
                    db, verify=True, username=cfg.arango_user, password=cfg.arango_pwd)
            else:
                rdb = await cli.db(db, verify=True)
            return rdb
        except aioarango.exceptions.ServerConnectionError as e:
            print(e)
    raise ValueError(f"Could not connect to Arango at {cfg.arango_url}")


_router = APIRouter()
_authheader = KBaseHTTPBearer()


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
    # TODO DOCS document error structure, see https://fastapi.tiangolo.com/advanced/additional-responses/
    # Will need to do this for each endpoint unfortunately
    # Also see https://github.com/tiangolo/fastapi/issues/1376
    content = {
        "httpcode": status_code,
        "httpstatus": responses[status_code],
        "time": _timestamp()
    }
    if error_type:
        content.update({
            "appcode": error_type.error_code, "apperror": error_type.error_type})
    if message:
        content.update({"message": message})
    if request_validation_detail:
        content.update({"request_validation_detail": request_validation_detail})
    return JSONResponse(status_code=status_code, content=jsonable_encoder({"error": content}))


def _timestamp():
    return datetime.now(timezone.utc).isoformat()


def _ensure_admin(user: KBaseUser, err_msg: str):
    if user.admin_perm != kb_auth.AdminPermission.FULL:
        raise errors.UnauthorizedError(err_msg)


def _get_storage(r: Request):
    return r.app.state.storage


def _err_on_control_chars(s: str, name: str):
    pos = contains_control_characters(s)
    if pos > -1:
        raise errors.IllegalParameterError(
            f"{name} contains a control character at position {pos}")


class Root(BaseModel):
    service_name: str = Field(example=SERVICE_NAME)
    version: str = Field(example=VERSION)
    git_hash: str = Field(example="b78f6e15e85381a7df71d6005d99e866f3f868dc")
    server_time: str = Field(example="2022-10-07T17:58:53.188698+00:00")


class WhoAmI(BaseModel):
    user: str = Field(example="kbasehelp")
    is_service_admin: bool = Field(example=False)


@_router.get("/", response_model=Root, tags=["General"])
async def root():
    return {
        "service_name": SERVICE_NAME,
        "version": VERSION,
        "git_hash": GIT_COMMIT,
        "server_time": _timestamp()
    }


@_router.get("/whoami", response_model = WhoAmI, tags=["General"])
async def whoami(user: KBaseUser=Depends(_authheader)):
    return {
        "user": user.user.id,
        "is_service_admin": kb_auth.AdminPermission.FULL == user.admin_perm
    }


@_router.put(
    "/collections/{collection_id}/versions/{ver_tag}/",
    response_model=models.SavedCollection,
    tags=["Collections"]
)
async def save_collection(
    # TODO ERRORS get rid of the 422 error data structure in OpenAPI docs, not using that
    # https://github.com/tiangolo/fastapi/issues/1376
    r: Request,
    col: models.Collection,
    collection_id: str = Path(  # The example and description is duplicated in models
        min_length=1,
        max_length=20,
        regex=r"^\w+$",
        example="GTDB",
        description="The unique ID of the collection."
    ),
    ver_tag: str = Path(  # The example and description is duplicated in models
        min_length=1,
        max_length=50,
        # pydantic uses the stdlib re under the hood, which doesn't understand \pC, so
        # we check for control chars manually below
        regex=models.REGEX_NO_WHITESPACE,  
        example="r207.kbase.2",
        description="A user assigned unique but otherwise arbitrary tag for the collection "
            + "version."
    ),
    user: KBaseUser=Depends(_authheader)
):
    # Maybe the method implementations should go into a different module / class...
    # But the method implementation is intertwined with the path validation
    _ensure_admin(user, "Only collections service admins can save data")
    _err_on_control_chars(ver_tag, "ver_tag")
    doc = col.dict()
    store = _get_storage(r)
    exists = await store.has_collection_version_by_tag(collection_id, ver_tag)
    if exists:
        raise errors.CollectionVersionExistsError(
            f"There is already a collection {collection_id} with version {ver_tag}")
    # Yes, this is a race condition - it's possible for 2+ users to try and save the same
    # collection/tag at the same time. If 2+ threads are able to pass this check with the
    # same coll/tag, the first one to get to arango will save and the other ones will get
    # errors returned from arango idential to the error above.
    # The drawback is just that ver_num will be wasted and there will be a gap in the versions,
    # which is annoying but not a major issue. The exists check is mostly to prevent wasting the
    # ver_num if the same request is sent twice in a row.
    # ver_num getting wasted otherwise is extremely unlikely and not worth worrying about further.
    ver_num = await store.get_next_version(collection_id)
    doc.update({
        models.FIELD_COLLECTION_ID: collection_id,
        models.FIELD_VER_TAG: ver_tag,
        models.FIELD_VER_NUM: ver_num,
        models.FIELD_DATE_CREATE: _timestamp(),
        models.FIELD_USER_CREATE: user.user.id,
    })
    sc = models.SavedCollection.construct(**doc)
    await store.save_collection_version(sc)
    return sc


# TODO DOCS the response schema title is gross. How to fix?
@_router.get("/collections", response_model = list[models.ActiveCollection], tags=["Collections"])
async def collections(r: Request) -> list[models.ActiveCollection]:
    return await _get_storage(r).get_collections_active()
