"""
Routes for general collections endpoints, as opposed to endpoint for a particular data product.
"""

import hashlib
import json
import jsonschema
import secrets
import time
import uuid

from fastapi import APIRouter, Request, Depends, Path, Query, Header
from typing import Any
from pydantic import BaseModel, Field
from src.common.git_commit import GIT_COMMIT
from src.common.version import VERSION
from src.service import app_state
from src.service import app_state_data_structures
from src.service import data_product_specs
from src.service import errors
from src.service import kb_auth
from src.service import match_deletion
from src.service import match_retrieval
from src.service import models
from src.service.clients.workspace_client import Workspace
from src.service.http_bearer import KBaseHTTPBearer
from src.service.matchers.common_models import Matcher
from src.service.routes_common import PATH_VALIDATOR_COLLECTION_ID, err_on_control_chars
from src.service.storage_arango import ArangoStorage
from src.service.timestamp import timestamp
from src.service.workspace_wrapper import WorkspaceWrapper

# TODO CODE it's about time to start splitting this file up

SERVICE_NAME = "Collections Prototype"

ROUTER_GENERAL = APIRouter(tags=["General"])
ROUTER_COLLECTIONS = APIRouter(tags=["Collections"])
ROUTER_MATCHES = APIRouter(tags=["Matches"])
ROUTER_SELECTIONS = APIRouter(tags=["Selections"])
ROUTER_COLLECTIONS_ADMIN = APIRouter(tags=["Collection Administration"])
ROUTER_MATCH_ADMIN = APIRouter(tags=["Match Administration"], prefix="/matchadmin")

UTF_8 = "utf-8"

MAX_SELECTION_IDS = 10000

_AUTH = KBaseHTTPBearer()


def _ensure_admin(user: kb_auth.KBaseUser, err_msg: str):
    if user.admin_perm != kb_auth.AdminPermission.FULL:
        raise errors.UnauthorizedError(err_msg)


def _precheck_admin_and_get_storage(
    r: Request, user: kb_auth.KBaseUser, ver_tag: str, op: str
) -> ArangoStorage:
    _ensure_admin(user, f"Only collections service admins can {op}")
    err_on_control_chars(ver_tag, "ver_tag")
    return app_state.get_app_state(r).arangostorage


def _check_matchers_and_data_products(
    appstate: app_state.CollectionsState, col: models.Collection
):
    data_products = set([dp.product for dp in col.data_products])
    for dp in data_products:
        data_product_specs.get_data_product_spec(dp)  # throws an exception if missing
    for m in col.matchers:
        matcher = appstate.get_matcher(m.matcher)
        if not matcher:
            raise errors.NoSuchMatcherError(f"No such matcher: {m.matcher}")
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
    raise errors.NoRegisteredMatcherError(matcher_id)


async def _activate_collection_version(
    user: kb_auth.KBaseUser, store: ArangoStorage, col: models.SavedCollection
) -> models.ActiveCollection:
    doc = col.dict()
    doc.update({
        models.FIELD_DATE_ACTIVE: timestamp(),
        models.FIELD_USER_ACTIVE: user.user.id
    })
    ac = models.ActiveCollection.construct(**doc)
    await store.save_collection_active(ac)
    return ac


# assumes UPAs are sorted
def _calc_match_id_md5(
    matcher_id: str,
    collection_id: str,
    collection_ver: int,
    params: dict[str, Any],
    upas: list[str],
) -> str:
    # this would be better if it just happened automatically when constructing the pydantic
    # match object, but that doesn't seem to work well with pydantic
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


async def _save_selection(
    appstate: app_state_data_structures.CollectionsState,
    coll: models.SavedCollection,
    token: str,
    active_selection_id: str,
    selection_ids: list[str],
    overwrite: bool = False
):
    if len(selection_ids) > MAX_SELECTION_IDS:
        raise errors.IllegalParameterError(f"At most {MAX_SELECTION_IDS} can be submitted")
    internal_id = str(uuid.uuid4())
    now = appstate.get_epoch_ms()
    internal_sel = models.InternalSelection(
        internal_selection_id=internal_id,
        collection_id=coll.id,
        collection_ver=coll.ver_num,
        selection_ids=selection_ids,
        unmatched_ids=None,
        created=now,
        heartbeat=None,
        state=models.ProcessState.PROCESSING,
        state_updated=now,
    )
    active_sel = models.ActiveSelection(
        selection_id_hash=_hash_token(token),
        active_selection_id=active_selection_id,
        internal_selection_id=internal_id,
        last_access=now,
    )
    await appstate.arangostorage.save_selection_internal(internal_sel)
    await appstate.arangostorage.save_selection_active(active_sel, overwrite=overwrite)
    # TODO SELECTION start selection process


# maybe these should go in a different module
def _get_token():
    return "coll-selection-" + secrets.token_urlsafe()  # 256 bits by default


def _hash_token(token: str):
    return hashlib.sha256(token.encode(UTF_8)).hexdigest()


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

_PATH_MATCH_ID = Path(description="The ID of the match")

_QUERY_MAX_VER = Query(
    default=None,
    ge=1,  # gt doesn't seem to be working correctly as of now
    example=57,
    description="The maximum collection version to return. This can be used to page through "
        + "the versions"
)

_QUERY_MATCH_VERBOSE = Query(
    default=False,
    example=False,
    description="Whether to return the KBase workspace UPAs and the matching IDs along with "
        + "the other match information. These data may be much larger than the rest of the "
        + "match and aren't often needed; in most cases they can be ignored. When false, "
        + "the UPAs and matching ID lists will be empty."
)


_QUERY_SELECTION_VERBOSE = Query(
    default=False,
    example=False,
    description="Whether to return the selection IDs along with "
        + "the other selection information. These data may be much larger than the rest of the "
        + "selection and aren't often needed; in most cases they can be ignored. When false, "
        + "the selection ID list will be empty."
)

_HEADER_KBASE_COLLECTIONS_SELECTION = Header(
    description="The selection ID / token returned when creating a selection."
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


class SelectionInput(BaseModel):
    """A selection of data in a collection. """
    selection_ids: list[str] = Field(
        example=models.FIELD_SELECTION_EXAMPLE,
        description=models.FIELD_SELECTION_IDS_DESCRIPTION
    )


class Selection(SelectionInput):
    collection_id: str = Field(
        example="GTDB",
        description="The ID of the collection for the selection.",
    )
    collection_ver: int = Field(
        example=7,
        description="The version of the collection for which the selection was created."
    )
    state: models.ProcessState = Field(
        example=models.ProcessState.PROCESSING.value,
        description="The state of the selection process."
    )
    unmatched_ids: list[str] | None = Field(
        example=models.FIELD_SELECTION_EXAMPLE,
        description=models.FIELD_SELECTION_UNMATCHED_DESCRIPTION,
    )


class SelectionToken(BaseModel):
    """Contains a token that allows access to a selection. Keep the token secret."""
    selection_token: str = Field(
        description="An opaque, secret string that can be used to access selections."
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
async def whoami(user: kb_auth.KBaseUser=Depends(_AUTH)):
    return {
        "user": user.user.id,
        "is_service_admin": kb_auth.AdminPermission.FULL == user.admin_perm
    }


@ROUTER_COLLECTIONS.get("/collectionids/", response_model = CollectionIDs)
async def get_collection_ids(r: Request):
    ids = await app_state.get_app_state(r).arangostorage.get_collection_ids()
    return CollectionIDs(data=ids)


@ROUTER_COLLECTIONS.get("/collections/", response_model = CollectionsList)
async def list_collections(r: Request):
    cols = await app_state.get_app_state(r).arangostorage.get_collections_active()
    return CollectionsList(data=cols)


@ROUTER_COLLECTIONS.get("/collections/{collection_id}/", response_model=models.ActiveCollection)
async def get_collection(r: Request, collection_id: str = PATH_VALIDATOR_COLLECTION_ID
) -> models.ActiveCollection:
    return await app_state.get_app_state(r).arangostorage.get_collection_active(collection_id)


@ROUTER_COLLECTIONS.get("/collections/{collection_id}/matchers", response_model=MatcherList)
async def get_collection_matchers(r: Request, collection_id: str = PATH_VALIDATOR_COLLECTION_ID
) -> MatcherList:
    coll = await get_collection(r, collection_id)
    appstate = app_state.get_app_state(r)
    return {"data": [appstate.get_matcher(m.matcher) for m in coll.matchers]}


@ROUTER_COLLECTIONS.post(
    "/collections/{collection_id}/matchers/{matcher_id}",
    response_model=models.Match,
    description="Match KBase workspace data against a collection.\n\n"
        + f"At most {match_retrieval.MAX_UPAS} objects may be submitted. "
        + "If sets are sumbitted, the set is expanded from the list of references in the set "
        + "returned by the workspace, ignoring any context for the references."
)
async def match(
    # could add a collection version endpoint so admins could try matches on non-active colls
    r: Request,
    match_params: MatchParameters,
    collection_id: str = PATH_VALIDATOR_COLLECTION_ID,
    matcher_id: str = Path(**models.MATCHER_ID_PROPS),  # is there a cleaner way to do this?
    user: kb_auth.KBaseUser=Depends(_AUTH),
) -> models.Match:
    coll = await get_collection(r, collection_id)
    matcher_info = _get_matcher_from_collection(coll, matcher_id)
    appstate = app_state.get_app_state(r)
    ws = appstate.get_workspace_client(user.token)
    matcher = appstate.get_matcher(matcher_info.matcher)
    match_process, upas, wsids = await match_retrieval.create_match_process(
        matcher,
        WorkspaceWrapper(ws),
        match_params.upas,
        match_params.parameters,
        matcher_info.parameters,
    )
    perm_check = appstate.get_epoch_ms()
    params = match_params.parameters or {}
    int_match = models.InternalMatch(
        match_id=_calc_match_id_md5(matcher_id, collection_id, coll.ver_num, params, upas),
        matcher_id=matcher_id,
        collection_id=coll.id,
        collection_ver=coll.ver_num,
        user_parameters=params,
        collection_parameters=matcher_info.parameters,
        state=models.ProcessState.PROCESSING,
        state_updated=perm_check,
        upas=upas,
        matches=[],
        internal_match_id=str(uuid.uuid4()),
        wsids=wsids,
        created=perm_check,
        last_access=perm_check,
        user_last_perm_check={user.user.id: perm_check}
    )
    curr_match, exists = await appstate.arangostorage.save_match(int_match)
    # don't bother checking if the match heartbeat is old here, just do it in the access methods
    if not exists:
        match_process.start(curr_match.match_id, appstate.get_pickleable_dependencies())
    return curr_match


@ROUTER_COLLECTIONS.post(
    "/collections/{collection_id}/selections",
    response_model=SelectionToken,
    summary="Create a data selection",
    description="Create a data selection, returning a selection token that can be used to "
        + "access the selection. Keep the token secret and safe to avoid rampaging wizards."
)
async def create_selection(
    r: Request,
    selection: SelectionInput,
    collection_id: str = PATH_VALIDATOR_COLLECTION_ID,
) -> SelectionToken:
    appstate = app_state.get_app_state(r)
    coll = await appstate.arangostorage.get_collection_active(collection_id)
    token = _get_token()
    await _save_selection(appstate, coll, token, str(uuid.uuid4()), selection.selection_ids)
    return SelectionToken(selection_token=token)


@ROUTER_MATCHES.get(
    "/matchers/",
    response_model=MatcherList,
    summary="Get matchers available in the system",
    description="List all matchers available in the service.",
)
async def matchers(r: Request) -> MatcherList:
    return {"data": app_state.get_app_state(r).get_matchers()}


@ROUTER_MATCHES.get(
    "/matches/{match_id}/",
    response_model=models.MatchVerbose,
    summary="Get a match",
    description="Get the status of a particular match.",
)
async def get_match(
    r: Request,
    match_id: str = _PATH_MATCH_ID,
    verbose: bool = _QUERY_MATCH_VERBOSE,
    user: kb_auth.KBaseUser = Depends(_AUTH),
) -> models.MatchVerbose:
    return await match_retrieval.get_match(
        app_state.get_app_state(r),
        match_id,
        user,
        verbose=verbose
    )


@ROUTER_SELECTIONS.get(
    "/selections/",
    response_model=Selection,
    summary="Get a selection",
    description="Get the status and contents of a selection."
)
async def get_selection(
    r: Request,
    # When I use an alias="" argument in the Header so I can use a sane name for the variable
    # the server thinks the header is missing
    KBASE_COLLECTIONS_SELECTION: str = _HEADER_KBASE_COLLECTIONS_SELECTION,
    verbose: bool = _QUERY_SELECTION_VERBOSE,
) -> Selection:
    appstate = app_state.get_app_state(r)
    hashed_token = _hash_token(KBASE_COLLECTIONS_SELECTION)
    active_sel = await appstate.arangostorage.get_selection_active(hashed_token)
    # could save bandwidth by passing verbose to DB layer and not pulling IDs
    internal_sel = await appstate.arangostorage.get_selection_internal(
        active_sel.internal_selection_id)
    # TODO SELECTION if the process heartbeat is dead, restart the process
    #                put that in a new module and move most of this code there
    await appstate.arangostorage.update_selection_active_last_access(
        hashed_token, appstate.get_epoch_ms())
    if not verbose:
        internal_sel.selection_ids = []
        internal_sel.unmatched_ids = None if internal_sel.unmatched_ids is None else []
    return Selection(
        selection_ids=internal_sel.selection_ids,
        unmatched_ids=internal_sel.unmatched_ids,
        collection_id=internal_sel.collection_id,
        collection_ver=internal_sel.collection_ver,
        state=internal_sel.state,
    )


@ROUTER_SELECTIONS.put(
    "/selections/",
    summary="Update a selection",
    description="Change a selection's selected data."
)
async def update_selection(
    r: Request,
    selection: SelectionInput,
    # When I use an alias="" argument in the Header so I can use a sane name for the variable
    # the server thinks the header is missing
    KBASE_COLLECTIONS_SELECTION: str = _HEADER_KBASE_COLLECTIONS_SELECTION,
):
    appstate = app_state.get_app_state(r)
    token = KBASE_COLLECTIONS_SELECTION
    active_sel = await appstate.arangostorage.get_selection_active(_hash_token(token))
    internal_sel = await appstate.arangostorage.get_selection_internal(
        active_sel.internal_selection_id)
    coll = await appstate.arangostorage.get_collection_active(internal_sel.collection_id)
    if coll.ver_num != internal_sel.collection_ver:
        raise errors.InvalidSelectionStateError(
            f"The requested selection is for {coll.id} collection version "
            + f"{internal_sel.collection_ver}, while the current version is {coll.ver_num}")
    await _save_selection(
        appstate,
        coll,
        token,
        active_sel.active_selection_id,
        selection.selection_ids,
        overwrite=True,
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
    user: kb_auth.KBaseUser=Depends(_AUTH)
) -> models.SavedCollection:
    # Maybe the method implementations should go into a different module / class...
    # But the method implementation is intertwined with the path validation
    store = _precheck_admin_and_get_storage(r, user, ver_tag, "save data")
    _check_matchers_and_data_products(app_state.get_app_state(r), col)
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
async def get_all_collection_ids(r: Request, user: kb_auth.KBaseUser=Depends(_AUTH)):
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
    user: kb_auth.KBaseUser=Depends(_AUTH)
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
    user: kb_auth.KBaseUser=Depends(_AUTH)
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
    user: kb_auth.KBaseUser=Depends(_AUTH)
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
    user: kb_auth.KBaseUser=Depends(_AUTH)
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
    user: kb_auth.KBaseUser=Depends(_AUTH),
) -> CollectionVersions:
    store = _precheck_admin_and_get_storage(r, user, "", "view collection versions")
    versions = await store.get_collection_versions(collection_id, max_ver=max_ver)
    counter = await store.get_current_version(collection_id)
    return CollectionVersions(counter=counter, data=versions)


# TODO ROUTES add a admin route to get a match without updating its timestamps etc.
#             for now just use the ArangoDB UI or API.


@ROUTER_MATCH_ADMIN.delete(
    "/{match_id}/",
    response_model=models.MatchVerbose,
    summary="!!! Danger !!! Delete a match",
    description="Delete a match, regardless of state. **BE SURE YOU KNOW WHAT YOU'RE DOING**. "
        + "Deleting a match when match processes are running can leave the database in an "
        + "inconsistent state and cause user errors or corrupted results. Even if processes are "
        + "not running, a recent request by a user can result in an error or corrupted results "
        + "if a match deletion occurs at the same time.",
)
async def delete_match(
    r: Request,
    match_id: str = _PATH_MATCH_ID,
    verbose: bool = _QUERY_MATCH_VERBOSE,
    user: kb_auth.KBaseUser = Depends(_AUTH),
) -> models.MatchVerbose:
    _ensure_admin(user, "Only collections service admins can delete matches")
    appstate = app_state.get_app_state(r)
    store = appstate.arangostorage
    match = await store.get_match_full(match_id)
    await match_deletion.move_match_to_deleted_state(store, match, appstate.get_epoch_ms())
    match = models.MatchVerbose(
        **models.remove_non_model_fields(match.dict(), models.MatchVerbose))
    if not verbose:
        # TODO PERF do this by not requesting the fields from the DB
        match.upas = []
        match.matches = []
    return match
