"""
Routes for general collections endpoints, as opposed to endpoint for a particular data product.
"""

from fastapi import APIRouter, Request, Depends, Path
from pydantic import BaseModel, Field
from src.common.git_commit import GIT_COMMIT
from src.common.version import VERSION
from src.service import app_state
from src.service import errors
from src.service import kb_auth
from src.service import models
from src.service.http_bearer import KBaseHTTPBearer, KBaseUser
from src.service.arg_checkers import contains_control_characters
from src.service.storage_arango import ArangoStorage
from src.service.timestamp import timestamp

SERVICE_NAME = "Collections Prototype"

ROUTER = APIRouter()

_authheader = KBaseHTTPBearer()

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
        models.FIELD_DATE_ACTIVE: timestamp(),
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
_TAG_COLLECTIONS_ADMIN = "Collection Administration"


@ROUTER.get("/", response_model=Root, tags=[_TAG_GENERAL])
async def root():
    return {
        "service_name": SERVICE_NAME,
        "version": VERSION,
        "git_hash": GIT_COMMIT,
        "server_time": timestamp()
    }


@ROUTER.get("/whoami", response_model = WhoAmI, tags=[_TAG_GENERAL])
async def whoami(user: KBaseUser=Depends(_authheader)):
    return {
        "user": user.user.id,
        "is_service_admin": kb_auth.AdminPermission.FULL == user.admin_perm
    }


# TODO DOCS the response schema title is gross. How to fix?
@ROUTER.get(
    "/collections",
    name = "List collections",
    response_model = CollectionsList,
    tags=[_TAG_COLLECTIONS])
async def collections(r: Request):
    cols = await app_state.get_storage(r).get_collections_active()
    return {"colls": cols}


@ROUTER.get(
    "/collections/{collection_id}/",
    response_model=models.ActiveCollection,
    tags=[_TAG_COLLECTIONS])
async def get_collection(r: Request, collection_id: str = _PATH_COLLECTION_ID
) -> models.ActiveCollection:
    return await app_state.get_storage(r).get_collection_active(collection_id)


@ROUTER.put(
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
) -> models.SavedCollection:
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
        models.FIELD_DATE_CREATE: timestamp(),
        models.FIELD_USER_CREATE: user.user.id,
    })
    sc = models.SavedCollection.construct(**doc)
    await store.save_collection_version(sc)
    return sc


@ROUTER.get(
    "/collections/{collection_id}/versions/tag/{ver_tag}/",
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


@ROUTER.put(
    "/collections/{collection_id}/versions/tag/{ver_tag}/activate/",
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


@ROUTER.get(
    "/collections/{collection_id}/versions/num/{ver_num}/",
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


@ROUTER.put(
    "/collections/{collection_id}/versions/num/{ver_num}/activate/",
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