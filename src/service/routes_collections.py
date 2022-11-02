"""
Routes for general collections endpoints, as opposed to endpoint for a particular data product.
"""

from fastapi import APIRouter, Request, Depends, Path, Query
from pydantic import BaseModel, Field
from src.common.git_commit import GIT_COMMIT
from src.common.version import VERSION
from src.service import app_state
from src.service import errors
from src.service import kb_auth
from src.service import models
from src.service.http_bearer import KBaseHTTPBearer, KBaseUser
from src.service.routes_common import PATH_VALIDATOR_COLLECTION_ID, err_on_control_chars
from src.service.storage_arango import ArangoStorage
from src.service.timestamp import timestamp

SERVICE_NAME = "Collections Prototype"

ROUTER_GENERAL = APIRouter(tags=["General"])
ROUTER_COLLECTIONS = APIRouter(tags=["Collections"])
ROUTER_COLLECTIONS_ADMIN = APIRouter(tags=["Collection Administration"])

_authheader = KBaseHTTPBearer()

def _ensure_admin(user: KBaseUser, err_msg: str):
    if user.admin_perm != kb_auth.AdminPermission.FULL:
        raise errors.UnauthorizedError(err_msg)


def _precheck_admin_and_get_storage(
    r: Request, user: KBaseUser, ver_tag: str, op: str
) -> ArangoStorage:
    _ensure_admin(user, f"Only collections service admins can {op}")
    err_on_control_chars(ver_tag, "ver_tag")
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

_QUERY_MAX_VER = Query(
    default=None,
    ge=1,  # gt doesn't seem to be working correctly as of now
    example=57,
    description="The maximum collection version to return. This can be used to page through "
        + "the versions"
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
    data: list[models.ActiveCollection]


class CollectionIDs(BaseModel):
    data: list[str] = Field(
        example=["GTDB", "PMI"],
        description="A list of collection IDs"
    )


class CollectionVersions(BaseModel):
    counter: int = Field(
        example=42,
        description="The value of the version counter for the collection, indicating the "
             + "maximum version that could possibly exist"
    )
    data: list[models.SavedCollection]


@ROUTER_GENERAL.get("/", response_model=Root)
async def root():
    return {
        "service_name": SERVICE_NAME,
        "version": VERSION,
        "git_hash": GIT_COMMIT,
        "server_time": timestamp()
    }


@ROUTER_GENERAL.get("/whoami", response_model = WhoAmI)
async def whoami(user: KBaseUser=Depends(_authheader)):
    return {
        "user": user.user.id,
        "is_service_admin": kb_auth.AdminPermission.FULL == user.admin_perm
    }


@ROUTER_COLLECTIONS.get("/collectionids", response_model = CollectionIDs)
async def get_collection_ids(r: Request):
    ids = await app_state.get_storage(r).get_collection_ids()
    return CollectionIDs(data=ids)


@ROUTER_COLLECTIONS.get("/collections", response_model = CollectionsList)
async def list_collections(r: Request):
    cols = await app_state.get_storage(r).get_collections_active()
    return CollectionsList(data=cols)


@ROUTER_COLLECTIONS.get("/collections/{collection_id}/", response_model=models.ActiveCollection)
async def get_collection(r: Request, collection_id: str = PATH_VALIDATOR_COLLECTION_ID
) -> models.ActiveCollection:
    return await app_state.get_storage(r).get_collection_active(collection_id)


@ROUTER_COLLECTIONS_ADMIN.put(
    "/collections/{collection_id}/versions/{ver_tag}/",
    response_model=models.SavedCollection,
    description="Save a collection version, which is initially inactive and and can be activated "
        + "via the admin endpoints.\n\n"
        + "**NOTE**: the service has no way to tell whether the collection is mixing different "
        + "versions of the source data in the data products; it is up to the user to "
        + "ensure that data products for a single collection version are all using the same "
        + "version of the source data."
)
async def save_collection(
    r: Request,
    col: models.Collection,
    collection_id: str = PATH_VALIDATOR_COLLECTION_ID,
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


@ROUTER_COLLECTIONS_ADMIN.get("/collectionids/all/", response_model = CollectionIDs)
async def get_all_collection_ids(r: Request, user: KBaseUser=Depends(_authheader)):
    store = _precheck_admin_and_get_storage(r, user, "", "view collection versions")
    ids = await store.get_collection_ids(all_=True)
    return CollectionIDs(data=ids)


@ROUTER_COLLECTIONS_ADMIN.get(
    "/collections/{collection_id}/versions/tag/{ver_tag}/",
    response_model=models.SavedCollection,
)
async def get_collection_by_ver_tag(
    r: Request,
    collection_id: str = PATH_VALIDATOR_COLLECTION_ID,
    ver_tag: str = _PATH_VER_TAG,
    user: KBaseUser=Depends(_authheader)
) -> models.SavedCollection:
    store = _precheck_admin_and_get_storage(r, user, ver_tag, "view collection versions")
    return await store.get_collection_version_by_tag(collection_id, ver_tag)


@ROUTER_COLLECTIONS_ADMIN.put(
    "/collections/{collection_id}/versions/tag/{ver_tag}/activate/",
    response_model=models.ActiveCollection,
)
async def activate_collection_by_ver_tag(
    r: Request,
    collection_id: str = PATH_VALIDATOR_COLLECTION_ID,
    ver_tag: str = _PATH_VER_TAG,
    user: KBaseUser=Depends(_authheader)
) -> models.ActiveCollection:
    store = _precheck_admin_and_get_storage(r, user, ver_tag, "activate a collection version")
    col = await store.get_collection_version_by_tag(collection_id, ver_tag)
    return await _activate_collection_version(user, store, col)


@ROUTER_COLLECTIONS_ADMIN.get(
    "/collections/{collection_id}/versions/num/{ver_num}/",
    response_model=models.SavedCollection,
)
async def get_collection_by_ver_num(
    r: Request,
    collection_id: str = PATH_VALIDATOR_COLLECTION_ID,
    ver_num: int = _PATH_VER_NUM,
    user: KBaseUser=Depends(_authheader)
) -> models.SavedCollection:
    store = _precheck_admin_and_get_storage(r, user, "", "view collection versions")
    return await store.get_collection_version_by_num(collection_id, ver_num)


@ROUTER_COLLECTIONS_ADMIN.put(
    "/collections/{collection_id}/versions/num/{ver_num}/activate/",
    response_model=models.ActiveCollection,
)
async def activate_collection_by_ver_num(
    r: Request,
    collection_id: str = PATH_VALIDATOR_COLLECTION_ID,
    ver_num: int = _PATH_VER_NUM,
    user: KBaseUser=Depends(_authheader)
) -> models.ActiveCollection:
    store = _precheck_admin_and_get_storage(r, user, "", "activate a collection version")
    col = await store.get_collection_version_by_num(collection_id, ver_num)
    return await _activate_collection_version(user, store, col)


@ROUTER_COLLECTIONS_ADMIN.get(
    "/collections/{collection_id}/versions",
    response_model=CollectionVersions,
    description="Get the list of versions for a collection, sorted in descending order "
        + "of the version number. Returns at most 1000 versions."
)
async def get_collection_versions(
    r: Request,
    collection_id: str = PATH_VALIDATOR_COLLECTION_ID,
    max_ver: int | None = _QUERY_MAX_VER,
    user: KBaseUser=Depends(_authheader),
) -> CollectionVersions:
    store = _precheck_admin_and_get_storage(r, user, "", "view collection versions")
    versions = await store.get_collection_versions(collection_id, max_ver=max_ver)
    counter = await store.get_current_version(collection_id)
    return CollectionVersions(counter=counter, data=versions)
