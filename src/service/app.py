'''
API for the collections service.
'''

import os
import sys

from datetime import datetime, timezone
from fastapi import FastAPI, Depends, Request, status, APIRouter, Path
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError, HTTPException
from fastapi.responses import JSONResponse
from http.client import responses
from pydantic import BaseModel, Field
from starlette.exceptions import HTTPException as StarletteHTTPException

from src.common.git_commit import GIT_COMMIT
from src.common.version import VERSION
from src.service import app_state
from src.service import kb_auth
from src.service import errors
from src.service import models
from src.service import models_errors
from src.service.arg_checkers import contains_control_characters
from src.service.config import CollectionsServiceConfig
from src.service.http_bearer import KBaseHTTPBearer, KBaseUser
from src.service.storage_arango import ArangoStorage


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
    app.include_router(_router)

    async def build_app_wrapper():
        await app_state.build_app(app, cfg)
    app.add_event_handler("startup", build_app_wrapper)

    async def clean_app_wrapper():
        await app_state.clean_app(app)
    app.add_event_handler("shutdown", clean_app_wrapper)
    return app


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


def _err_on_control_chars(s: str, name: str):
    pos = contains_control_characters(s)
    if pos > -1:
        raise errors.IllegalParameterError(
            f"{name} contains a control character at position {pos}")


def _precheck_admin_and_get_storage(
    r: Request, user: KBaseUser, ver_tag: str, op: str
) -> ArangoStorage:
    _ensure_admin(user, f"Only collections service admins can {op}")
    _err_on_control_chars(ver_tag, "ver_tag")
    return app_state.get_storage(r)


async def _activate_collection_version(
    user: KBaseUser, store: ArangoStorage, col: models.SavedCollection
) -> models.ActiveCollection:
    doc = col.dict()
    doc.update({
        models.FIELD_DATE_ACTIVE: _timestamp(),
        models.FIELD_USER_ACTIVE: user.user.id
    })
    ac = models.ActiveCollection.construct(**doc)
    await store.save_collection_active(ac)
    return ac


_PATH_COLLECTION_ID = Path(
    min_length=1,
    max_length=20,
    regex=r"^\w+$",
    example=models.FIELD_COLLECTION_ID_EXAMPLE,
    description=models.FIELD_COLLECTION_ID_DESCRIPTION
)


_PATH_VER_TAG = Path(
    min_length=1,
    max_length=50,
    # pydantic uses the stdlib re under the hood, which doesn't understand \pC, so
    # routes need to manually check for control characters
    regex=models.REGEX_NO_WHITESPACE,  
    example=models.FIELD_VER_TAG_EXAMPLE,
    description=models.FIELD_VER_TAG_DESCRIPTION
)


_PATH_VER_NUM = Path(
    ge=1,  # gt doesn't seem to be working correctly as of now
    example=models.FIELD_VER_NUM_EXAMPLE,
    description=models.FIELD_VER_NUM_DESCRIPTION
)


class Root(BaseModel):
    service_name: str = Field(example=SERVICE_NAME)
    version: str = Field(example=VERSION)
    git_hash: str = Field(example="b78f6e15e85381a7df71d6005d99e866f3f868dc")
    server_time: str = Field(example="2022-10-07T17:58:53.188698+00:00")


class WhoAmI(BaseModel):
    user: str = Field(example="kbasehelp")
    is_service_admin: bool = Field(example=False)


class CollectionsList(BaseModel):
    colls: list[models.ActiveCollection]


_TAG_GENERAL = "General"
_TAG_COLLECTIONS = "Collections"
_TAG_COLLECTIONS_ADMIN = "Collections, Admin"


@_router.get("/", response_model=Root, tags=[_TAG_GENERAL])
async def root():
    return {
        "service_name": SERVICE_NAME,
        "version": VERSION,
        "git_hash": GIT_COMMIT,
        "server_time": _timestamp()
    }


@_router.get("/whoami", response_model = WhoAmI, tags=[_TAG_GENERAL])
async def whoami(user: KBaseUser=Depends(_authheader)):
    return {
        "user": user.user.id,
        "is_service_admin": kb_auth.AdminPermission.FULL == user.admin_perm
    }


# TODO DOCS the response schema title is gross. How to fix?
@_router.get(
    "/collections",
    name = "List collections",
    response_model = CollectionsList,
    tags=[_TAG_COLLECTIONS])
async def collections(r: Request):
    cols = await app_state.get_storage(r).get_collections_active()
    return {"colls": cols}


@_router.put(
    "/collections/{collection_id}/versions/{ver_tag}/",
    response_model=models.SavedCollection,
    tags=[_TAG_COLLECTIONS_ADMIN]
)
async def save_collection(
    r: Request,
    col: models.Collection,
    collection_id: str = _PATH_COLLECTION_ID,
    ver_tag: str = _PATH_VER_TAG,
    user: KBaseUser=Depends(_authheader)
):
    # Maybe the method implementations should go into a different module / class...
    # But the method implementation is intertwined with the path validation
    store = _precheck_admin_and_get_storage(r, user, ver_tag, "save data")
    doc = col.dict()
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


@_router.get("/collections/{collection_id}/versions/tag/{ver_tag}/",
    response_model=models.SavedCollection,
    tags=[_TAG_COLLECTIONS_ADMIN]
)
async def get_collection_by_ver_tag(
    r: Request,
    collection_id: str = _PATH_COLLECTION_ID,
    ver_tag: str = _PATH_VER_TAG,
    user: KBaseUser=Depends(_authheader)
) -> models.SavedCollection:
    store = _precheck_admin_and_get_storage(r, user, ver_tag, "view collection versions")
    return await store.get_collection_version_by_tag(collection_id, ver_tag)


@_router.put("/collections/{collection_id}/versions/tag/{ver_tag}/activate/",
    response_model=models.ActiveCollection,
    tags=[_TAG_COLLECTIONS_ADMIN]
)
async def activate_collection_by_ver_tag(
    r: Request,
    collection_id: str = _PATH_COLLECTION_ID,
    ver_tag: str = _PATH_VER_TAG,
    user: KBaseUser=Depends(_authheader)
) -> models.ActiveCollection:
    store = _precheck_admin_and_get_storage(r, user, ver_tag, "activate a collection version")
    col = await store.get_collection_version_by_tag(collection_id, ver_tag)
    return await _activate_collection_version(user, store, col)


@_router.get("/collections/{collection_id}/versions/num/{ver_num}/",
    response_model=models.SavedCollection,
    tags=[_TAG_COLLECTIONS_ADMIN]
)
async def get_collection_by_ver_num(
    r: Request,
    collection_id: str = _PATH_COLLECTION_ID,
    ver_num: int = _PATH_VER_NUM,
    user: KBaseUser=Depends(_authheader)
) -> models.SavedCollection:
    store = _precheck_admin_and_get_storage(r, user, "", "view collection versions")
    return await store.get_collection_version_by_num(collection_id, ver_num)


@_router.put("/collections/{collection_id}/versions/num/{ver_num}/activate/",
    response_model=models.ActiveCollection,
    tags=[_TAG_COLLECTIONS_ADMIN]
)
async def activate_collection_by_ver_num(
    r: Request,
    collection_id: str = _PATH_COLLECTION_ID,
    ver_num: int = _PATH_VER_NUM,
    user: KBaseUser=Depends(_authheader)
) -> models.ActiveCollection:
    store = _precheck_admin_and_get_storage(r, user, "", "activate a collection version")
    col = await store.get_collection_version_by_num(collection_id, ver_num)
    return await _activate_collection_version(user, store, col)


# TODO DOCS note firmly that collection versions shouldn't mix data from different
# versions of the source data
# TODO REFACTOR auth handling. Calling check_admin anyway, might was well do it all there
# rather than with an overly complex Depends... but that's needed for OpenAPI to know about
# the header. hmm. Also may need to be optional for some cases, now it's all or nothing
# https://stackoverflow.com/questions/70926257/how-to-pass-authorization-header-from-swagger-doc-in-python-fast-api
# TODO REFACTOR move routes into separate modules, routes and routes_common for things
# that data products may need
# TODO API get active collection
# TODO API get max version / versions of collection, needs paging
# TODO STRUCTURE figure out how to structure data product code, ideally shoudln't touch main code
