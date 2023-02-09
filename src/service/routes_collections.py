"""
Routes for general collections endpoints, as opposed to endpoint for a particular data product.
"""

import hashlib
import json
import jsonschema
import time
import uuid

from fastapi import APIRouter, Request, Depends, Path, Query
from typing import Any
from pydantic import BaseModel, Field
from src.common.git_commit import GIT_COMMIT
from src.common.version import VERSION
from src.service import app_state
from src.service import data_product_specs
from src.service import errors
from src.service import kb_auth
from src.service import matcher_registry
from src.service import match_retrieval
from src.service import models
from src.service.clients.workspace_client import Workspace
from src.service.http_bearer import KBaseHTTPBearer, KBaseUser
from src.service.matchers.common_models import Matcher
from src.service.routes_common import PATH_VALIDATOR_COLLECTION_ID, err_on_control_chars
from src.service.storage_arango import ArangoStorage
from src.service.timestamp import timestamp, now_epoch_millis
from src.service.workspace_wrapper import WorkspaceWrapper

SERVICE_NAME = "Collections Prototype"

ROUTER_GENERAL = APIRouter(tags=["General"])
ROUTER_MATCHES = APIRouter(tags=["Matches"])
ROUTER_COLLECTIONS = APIRouter(tags=["Collections"])
ROUTER_COLLECTIONS_ADMIN = APIRouter(tags=["Collection Administration"])

MAX_UPAS = 10000

UTF_8 = "utf-8"

_AUTH = KBaseHTTPBearer()


def _ensure_admin(user: KBaseUser, err_msg: str):
    if user.admin_perm != kb_auth.AdminPermission.FULL:
        raise errors.UnauthorizedError(err_msg)


def _precheck_admin_and_get_storage(
    r: Request, user: KBaseUser, ver_tag: str, op: str
) -> ArangoStorage:
    _ensure_admin(user, f"Only collections service admins can {op}")
    err_on_control_chars(ver_tag, "ver_tag")
    return app_state.get_storage(r)


def _check_matchers_and_data_products(col: models.Collection):
    data_products = set([dp.product for dp in col.data_products])
    for dp in data_products:
        if dp not in data_product_specs.DATA_PRODUCTS:
            raise errors.NoSuchDataProduct(f"No such data product: {dp}")
    for m in col.matchers:
        matcher = matcher_registry.MATCHERS.get(m.matcher)
        if not matcher:
            raise errors.NoSuchMatcher(f"No such matcher: {m.matcher}")
        missing_dps = set(matcher.required_data_products) - data_products
        if missing_dps:
            raise errors.IllegalParameterError(
                f"Matcher {matcher.id} requires data products {sorted(missing_dps)}")
        if matcher.collection_parameters:
            try:
                jsonschema.validate(instance=m.parameters, schema=matcher.collection_parameters)
            except jsonschema.exceptions.ValidationError as e:
                raise errors.IllegalParameterError(
                    # TODO MATCHERS str(e) is pretty gnarly. Figure out a nicer representation
                    f"Failed to validate parameters for matcher {matcher.id}: {e}")


def _get_matcher_from_collection(collection: models.SavedCollection, matcher_id: str
) -> models.Matcher:
    for m in collection.matchers:
        if m.matcher == matcher_id:
            return m
    raise errors.NoRegisteredMatcher(matcher_id)


# Assumes matcher_info is valid
def _check_perms_and_setup_match_process(
    ws: Workspace,
    upas: list[str],
    matcher_info: models.Matcher):
    # All matchers will need to check permissions and deletion state for the workspace objects,
    # so we get the metadata which is the cheapest way to do that. Most matchers will need
    # the metadata anyway.
    # Getting the objects might be really expensive depending on the size and number, so we
    # leave that to the matchers themselves, which should probably start a ee2 (?) job if object
    # downloads are required

    # TODO MATCHERS if the workspace check only includes that workspace IDs, it's not checking
    #   for deleted objects. Maybe that's ok? Document it anyway
    matcher = matcher_registry.MATCHERS[matcher_info.matcher]
    meta = WorkspaceWrapper(ws).get_object_metadata(upas, allowed_types=matcher.types)
    return matcher.generate_match_process(
        {u: m for u, m in zip(upas, meta)},
        matcher_info.parameters,
    )


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


def _upaerror(upa, upapath, upapathlen, index):
    if upapathlen > 1:
        raise errors.IllegalParameterError(
            f"Illegal UPA '{upa}' in path '{upapath}' at index {index}")
    else:
        raise errors.IllegalParameterError(f"Illegal UPA '{upa}' at index {index}")


def _check_and_sort_UPAs_and_get_wsids(upas: list[str]) -> tuple[list[str], set[int]]:
    # We check UPA format prior to sending them to the workspace since figuring out the
    # type of the WS error is too much of a pain. Easier to figure out obvious errors first
    
    # Probably a way to make this faster / more compact w/ list comprehensions, but not worth
    # the time I was putting into it
    upa_parsed = []
    for i, upapath in enumerate(upas):
        upapath_parsed = []
        upapath_split = upapath.strip().split(";")
        # deal with trailing ';'
        upapath_split = upapath_split[:-1] if not upapath_split[-1] else upapath_split
        for upa in upapath_split:
            upaparts = upa.split("/")
            if len(upaparts) != 3:
                _upaerror(upa, upapath, len(upapath_split), i)
            try:
                upapath_parsed.append(tuple(int(part) for part in upaparts))
            except ValueError:
                _upaerror(upa, upapath, len(upapath_split), i)
        upa_parsed.append(upapath_parsed)
    upa_parsed = sorted(upa_parsed)
    wsids = {arr[0][0] for arr in upa_parsed}
    ret = []
    for path in upa_parsed:
        path_list = []
        for upa in path:
            path_list.append("/".join([str(x) for x in upa]))
        ret.append(";".join(path_list))
    return ret, wsids


# assumes UPAs are sorted
def _calc_match_id_md5(
    matcher_id: str,
    collection_id: str,
    collection_ver: int,
    params: dict[str, Any],
    upas: list[str],
) -> str:
    pipe = "|".encode(UTF_8)
    m = hashlib.md5()
    for var in [matcher_id, collection_id, str(collection_ver)]:
        m.update(var.encode(UTF_8))
        m.update(pipe)
    # this will not sort arrays, and arrays might have positional semantics so we shouldn't do
    # that anyway. If we have match parameters where sorting arrays is an issue we'll need
    # to implement on a per matcher basis.
    m.update(json.dumps(params, sort_keys=True).encode(UTF_8))
    m.update(pipe)
    for u in upas:
        m.update(u.encode(UTF_8))
        m.update(pipe)
    return m.hexdigest()
    

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


class MatcherList(BaseModel):
    data: list[Matcher]


class CollectionsList(BaseModel):
    data: list[models.ActiveCollection]


class CollectionIDs(BaseModel):
    data: list[str] = Field(
        example=["GTDB", "PMI"],
        description="A list of collection IDs"
    )


class MatchParameters(BaseModel):
    """ Parameters for a match of KBase Workspace objects to collection items. """
    upas: list[str] = Field(
        example=models.FIELD_UPA_LIST_EXAMPLE,
        description=models.FIELD_UPA_LIST_DESCRIPTION,
    )
    parameters: dict[str, Any] | None = Field(
        example=models.FIELD_USER_PARAMETERS_EXAMPLE,
        description=models.FIELD_USER_PARAMETERS_DESCRIPTION,
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


@ROUTER_GENERAL.get("/whoami/", response_model = WhoAmI)
async def whoami(user: KBaseUser=Depends(_AUTH)):
    return {
        "user": user.user.id,
        "is_service_admin": kb_auth.AdminPermission.FULL == user.admin_perm
    }


@ROUTER_COLLECTIONS.get("/collectionids/", response_model = CollectionIDs)
async def get_collection_ids(r: Request):
    ids = await app_state.get_storage(r).get_collection_ids()
    return CollectionIDs(data=ids)


@ROUTER_COLLECTIONS.get("/collections/", response_model = CollectionsList)
async def list_collections(r: Request):
    cols = await app_state.get_storage(r).get_collections_active()
    return CollectionsList(data=cols)


@ROUTER_COLLECTIONS.get("/collections/{collection_id}/", response_model=models.ActiveCollection)
async def get_collection(r: Request, collection_id: str = PATH_VALIDATOR_COLLECTION_ID
) -> models.ActiveCollection:
    return await app_state.get_storage(r).get_collection_active(collection_id)


@ROUTER_COLLECTIONS.get("/collections/{collection_id}/matchers", response_model=MatcherList)
async def get_collection_matchers(r: Request, collection_id: str = PATH_VALIDATOR_COLLECTION_ID
) -> MatcherList:
    coll = await get_collection(r, collection_id)
    return {"data": [matcher_registry.MATCHERS[m.matcher] for m in coll.matchers]}


@ROUTER_COLLECTIONS.post(
    "/collections/{collection_id}/matchers/{matcher_id}",
    response_model=models.Match,
    description="Match KBase workspace data against a collection.\n\n"
        + f"At most {MAX_UPAS} objects may be submitted."
)
async def match(
    # could add a collection version endpoint so admins could try matches on non-active colls
    r: Request,
    match_params: MatchParameters,
    collection_id: str = PATH_VALIDATOR_COLLECTION_ID,
    matcher_id: str = Path(**models.MATCHER_ID_PROPS),  # is there a cleaner way to do this?
    user: KBaseUser=Depends(_AUTH),
) -> models.Match:
    if len(match_params.upas) > MAX_UPAS:
        raise errors.IllegalParameterError(f"No more than {MAX_UPAS} UPAs are allowed per match")
    coll = await get_collection(r, collection_id)
    matcher_info = _get_matcher_from_collection(coll, matcher_id)
    # TODO MATCHERS check user parameters when a matcher needs them
    # TODO MATCHERS handle genome and assembly set types
    upas, wsids = _check_and_sort_UPAs_and_get_wsids(match_params.upas)
    ws = app_state.get_workspace(r, user.token)
    match_process = _check_perms_and_setup_match_process(ws, upas, matcher_info)
    perm_check = now_epoch_millis()
    params = match_params.parameters or {}
    int_match = models.InternalMatch(
        match_id=_calc_match_id_md5(matcher_id, collection_id, coll.ver_num, params, upas),
        matcher_id=matcher_id,
        collection_id=coll.id,
        collection_ver=coll.ver_num,
        user_parameters=params,
        match_state=models.MatchState.PROCESSING,
        match_state_updated=perm_check,
        upas=upas,
        matches=[],
        internal_match_id=str(uuid.uuid4()),
        wsids=wsids,
        created=perm_check,
        last_access=perm_check,
        user_last_perm_check={user.user.id: perm_check}
    )
    storage = app_state.get_storage(r)
    curr_match, exists = await storage.save_match(int_match)
    # don't bother checking if the match heartbeat is old here, just do it in the access methods
    if not exists:
        match_process.start(curr_match.match_id, app_state.get_pickleable_dependencies(r))
    return curr_match


@ROUTER_MATCHES.get(
    "/matchers/",
    response_model=MatcherList,
    summary="Get matchers available in the system",
    description="List all matchers available in the service.",
)
async def matchers():
    return {"data": list(matcher_registry.MATCHERS.values())}


@ROUTER_MATCHES.get(
    "/matches/{match_id}/",
    response_model=models.MatchVerbose,
    summary="Get a match",
    description="Get the status of a particular match.",
)
async def get_match(
    r: Request,
    match_id: str = Path(
        description="The ID of the match"
    ),
    verbose: bool = Query(
        default=False,
        example=False,
        description="Whether to return the KBase workspace UPAs and the matching IDs along with "
            + "the other match information. These data may be much larger than the rest of the "
            + "match and aren't often needed; in most cases they can be ignored. When false, "
            + "the UPAs and matching ID lists will be empty."
    ),
    user: KBaseUser = Depends(_AUTH),
) -> models.MatchVerbose:
    storage = app_state.get_storage(r)
    ws = app_state.get_workspace(r, user.token)
    return await match_retrieval.get_match(
        match_id,
        user.user.id,
        storage,
        WorkspaceWrapper(ws),
        verbose=verbose
    )


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
    user: KBaseUser=Depends(_AUTH)
) -> models.SavedCollection:
    # Maybe the method implementations should go into a different module / class...
    # But the method implementation is intertwined with the path validation
    store = _precheck_admin_and_get_storage(r, user, ver_tag, "save data")
    _check_matchers_and_data_products(col)
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
        models.FIELD_MATCHERS: sorted(
            doc[models.FIELD_MATCHERS],
            key=lambda m: m[models.FIELD_MATCHERS_MATCHER]),
        models.FIELD_DATA_PRODUCTS: sorted(
            doc[models.FIELD_DATA_PRODUCTS],
            key=lambda m: m[models.FIELD_DATA_PRODUCTS_PRODUCT]),
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
async def get_all_collection_ids(r: Request, user: KBaseUser=Depends(_AUTH)):
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
    user: KBaseUser=Depends(_AUTH)
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
    user: KBaseUser=Depends(_AUTH)
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
    user: KBaseUser=Depends(_AUTH)
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
    user: KBaseUser=Depends(_AUTH)
) -> models.ActiveCollection:
    store = _precheck_admin_and_get_storage(r, user, "", "activate a collection version")
    col = await store.get_collection_version_by_num(collection_id, ver_num)
    return await _activate_collection_version(user, store, col)


@ROUTER_COLLECTIONS_ADMIN.get(
    "/collections/{collection_id}/versions/",
    response_model=CollectionVersions,
    description="Get the list of versions for a collection, sorted in descending order "
        + "of the version number. Returns at most 1000 versions."
)
async def get_collection_versions(
    r: Request,
    collection_id: str = PATH_VALIDATOR_COLLECTION_ID,
    max_ver: int | None = _QUERY_MAX_VER,
    user: KBaseUser=Depends(_AUTH),
) -> CollectionVersions:
    store = _precheck_admin_and_get_storage(r, user, "", "view collection versions")
    versions = await store.get_collection_versions(collection_id, max_ver=max_ver)
    counter = await store.get_current_version(collection_id)
    return CollectionVersions(counter=counter, data=versions)
