"""
Routes for general collections endpoints, as opposed to endpoint for a particular data product.
"""

import asyncio
import jsonschema

from fastapi import APIRouter, Request, Depends, Path, Query, Body
from typing import Any, Annotated
from pydantic import BaseModel, Field
from src.common.git_commit import GIT_COMMIT
from src.common.version import VERSION
from src.service import app_state
from src.service import data_product_specs
from src.service import errors
from src.service import kb_auth
from src.service import models
from src.service import processing_matches
from src.service import processing_selections
from src.service.http_bearer import KBaseHTTPBearer
from src.service.matchers.common_models import Matcher
from src.service.routes_common import PATH_VALIDATOR_COLLECTION_ID, err_on_control_chars
from src.service.storage_arango import ArangoStorage
from src.service.timestamp import timestamp

# TODO CODE it's about time to start splitting this file up

SERVICE_NAME = "Collections Prototype"

ROUTER_GENERAL = APIRouter(tags=["General"])
ROUTER_COLLECTIONS = APIRouter(tags=["Collections"])
ROUTER_MATCHES = APIRouter(tags=["Matches"])
ROUTER_SELECTIONS = APIRouter(tags=["Selections"])
ROUTER_COLLECTIONS_ADMIN = APIRouter(tags=["Collection Administration"])
ROUTER_DANGER = APIRouter(tags=["Here be Dragons"])

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


async def _check_collection_state(
    appstate: app_state.CollectionsState, col: models.Collection
):
    for dp in col.data_products:
        # throws an exception if missing
        spec = data_product_specs.get_data_product_spec(dp.product)
        if spec.view_required():
            if not dp.search_view:
                raise errors.IllegalParameterError(
                    f"Data product {dp.product} requires an ArangoSearch view")
            if not await appstate.arangostorage.has_search_view(dp.search_view):
                raise errors.IllegalParameterError(
                    f"ArangoSearch view {dp.search_view} configured in data product {dp.product} "
                    + "does not exist")
            # theoretically we could load the specs for the data product and make sure the
            # view matches the specs but that seems like overkill for something only admins
            # can set
    data_products = set([dp.product for dp in col.data_products])
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


async def _activate_collection_version(
    user: kb_auth.KBaseUser, store: ArangoStorage, col: models.SavedCollection
) -> models.ActiveCollection:
    doc = col.dict()
    doc.update({
        # otherwise the pydantic validation chokes on the dicts
        models.FIELD_MATCHERS: col.matchers,
        models.FIELD_DATA_PRODUCTS: col.data_products,
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
    pattern=models.REGEX_NO_WHITESPACE,
    example=models.FIELD_VER_TAG_EXAMPLE,
    description=models.FIELD_VER_TAG_DESCRIPTION
)


_PATH_VER_NUM = Path(
    ge=1,  # gt doesn't seem to be working correctly as of now
    example=models.FIELD_VER_NUM_EXAMPLE,
    description=models.FIELD_VER_NUM_DESCRIPTION
)

_PATH_MATCH_ID = Path(description="The ID of the match", min_length=1)


_PATH_SELECTION_ID = Path(description="The ID of the selection", min_length=1)


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
    """ A selection of data in a collection. """
    selection_ids: list[str] = Field(
        example=models.FIELD_SELECTION_EXAMPLE,
        description=models.FIELD_SELECTION_IDS_DESCRIPTION
    )


class SelectionTypes(BaseModel):
    """ The set of types available for export to a workspace in a collection. """
    types: list[str] = Field(
        example=["KBaseGenomes.Genome", "KBaseGenomeAnnotations.Assembly"],
        description="The types of data available for export for this selection."
    )
# For now assume there's just one list per collection, and not different lists per data product
# or something like that.


class SetSaveInformation(BaseModel):
    """ Information to provide when saving a set. """
    description: str | None = Field(
        max_length=800,
        description="An arbitrary description of a set, no more than 800 bytes."
    )

class WorkspaceSet(BaseModel):
    """ Information about a set in the KBase workspace service. """
    upa: str = Field(
        example="67/2/3",
        description="The UPA of the set."
    )
    type: str = Field(
        example="KBaseSets.AssemblySet",
        description="The type of the set."
    )


class CreatedSet(BaseModel):
    """ A set created by the selection service. """
    set: WorkspaceSet = Field(description="The set created in the KBase workspace service")


class CollectionVersions(BaseModel):
    counter: int = Field(
        example=42,
        description="The value of the version counter for the collection, indicating the "
             + "maximum version that could possibly exist"
    )
    data: list[models.SavedCollection]


class DeletedSubsets(BaseModel):
    """
    IDs of the matches and selections that were moved to the deleted state.
    """
    match_ids: list[str] = Field(description="The match IDs")
    selection_ids: list[str] = Field(description="The selection IDs")


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
        + f"At most {processing_matches.MAX_UPAS} objects may be submitted. "
        + "If sets are submitted, the set is expanded from the list of references in the set "
        + "returned by the workspace, ignoring any context for the references."
)
async def match(
    # could add a collection version param so admins could try matches on non-active colls
    r: Request,
    match_params: MatchParameters,
    collection_id: str = PATH_VALIDATOR_COLLECTION_ID,
    matcher_id: str = Path(**models.MATCHER_ID_PROPS),  # is there a cleaner way to do this?
    user: kb_auth.KBaseUser=Depends(_AUTH),
) -> models.Match:
    appstate = app_state.get_app_state(r)
    return await processing_matches.create_match(
        appstate,
        collection_id,
        matcher_id,
        user,
        match_params.upas,
        match_params.parameters)


@ROUTER_COLLECTIONS.post(
    "/collections/{collection_id}/selections",
    response_model=models.Selection,
    summary="Create a data selection",
    description="Create a data selection."
)
async def create_selection(
    r: Request,
    selection: SelectionInput,
    collection_id: str = PATH_VALIDATOR_COLLECTION_ID,
) -> models.Selection:
    appstate = app_state.get_app_state(r)
    return await processing_selections.save_selection(
        appstate, collection_id, selection.selection_ids)


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
    return await processing_matches.get_match(
        app_state.get_app_state(r), match_id, user, verbose=verbose)


@ROUTER_SELECTIONS.get(
    "/selections/{selection_id}",
    response_model=models.SelectionVerbose,
    summary="Get a selection",
    description="Get the status and contents of a selection."
)
async def get_selection(
    r: Request,
    selection_id: str = _PATH_SELECTION_ID,
    verbose: bool = _QUERY_SELECTION_VERBOSE,
) -> models.SelectionVerbose:
    return await processing_selections.get_selection(
        app_state.get_app_state(r), selection_id, verbose=verbose
    )


@ROUTER_SELECTIONS.get(
    "/selections/{selection_id}/types",
    response_model=SelectionTypes,
    summary="Get exportable types for a selection",
    description="Get the types that are available for export to a workspace for this selection."
)
async def get_selection_types(
    r: Request,
    selection_id: str = _PATH_SELECTION_ID,
) -> SelectionTypes:
    types = await processing_selections.get_exportable_types(
        app_state.get_app_state(r), selection_id)
    return SelectionTypes(types=types)


@ROUTER_SELECTIONS.post(
    "/selections/{selection_id}/toset/{workspace_id}/obj/{object_name}/type/{ws_type}",
    response_model=CreatedSet,
    summary="Create a workspace set",
    description="Create a set in the KBase workspace service from the selection."
)
async def create_sets(
    r: Request,
    setinfo: SetSaveInformation | None = Body(default=None),
    selection_id: str = _PATH_SELECTION_ID,
    workspace_id: int = Path(
        example=7165,
        description="The ID of the workspace where the set should be saved.",
        ge=1,
    ),
    object_name: str = Path(
        example="MySetObject",
        description="The object name to use when saving the set object.",
        # https://github.com/kbase/workspace_deluxe/blob/4c03b4364a2ccc292f60ccd629cbb5f71b25bfcc/src/us/kbase/workspace/database/ObjectIDNoWSNoVer.java
        min_length=1,
        max_length=255,
        pattern=r"^[\w\.\|_-]+$"
    ),
    ws_type: str = Path(
        example="KBaseGenomeAnnotations.Assembly",
        description="The workspace type of the data to export.",
        min_length=1,
        max_length=255,
    ),
    # TODO SELECTION allow recording a match ID when making a selection or
    #                making a selection directly from a match?
    #                This'll work for now, improve later. Will complicate match / selection
    #                expiration a lot - should the match expire if a selection points to it?
    #                Maybe just record match params, matcher_id, & whether
    #                selection_ids == match_ids, which is the info we need here
    match_id: Annotated[str, Query(
        description="The match ID from which the selection was generated."
    )] = None,
    user: kb_auth.KBaseUser = Depends(_AUTH),
) -> CreatedSet:
    desc = setinfo.description if setinfo else None
    appstate = app_state.get_app_state(r)
    match_ = None
    if match_id:
        match_ = await processing_matches.get_match(appstate, match_id, user, verbose=True)
    upa, type_ = await processing_selections.save_set_to_workspace(
        appstate,
        selection_id,
        user,
        workspace_id,
        object_name,
        ws_type,
        service_ver=VERSION,
        description=desc,
        match_=match_, 
    )
    return CreatedSet(set=WorkspaceSet(upa=upa, type=type_))


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
    await _check_collection_state(app_state.get_app_state(r), col)
    doc = col.dict()
    exists = await store.has_collection_version_by_tag(collection_id, ver_tag)
    if exists:
        raise errors.CollectionVersionExistsError(
            f"There is already a collection {collection_id} with version {ver_tag}")
    # Yes, this is a race condition - it's possible for 2+ users to try and save the same
    # collection/tag at the same time. If 2+ threads are able to pass this check with the
    # same coll/tag, the first one to get to arango will save and the other ones will get
    # errors returned from arango identical to the error above.
    # The drawback is just that ver_num will be wasted and there will be a gap in the versions,
    # which is annoying but not a major issue. The exists check is mostly to prevent wasting the
    # ver_num if the same request is sent twice in a row.
    # ver_num getting wasted otherwise is extremely unlikely and not worth worrying about further.
    ver_num = await store.get_next_version(collection_id)
    doc.update({
        models.FIELD_MATCHERS: sorted(
            [models.Matcher.construct(**d) for d in doc[models.FIELD_MATCHERS]],
            key=lambda m: m.matcher),
        models.FIELD_DATA_PRODUCTS: sorted(
            [models.DataProduct.construct(**d) for d in doc[models.FIELD_DATA_PRODUCTS]],
            key=lambda m: m.product),
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
    max_ver: int = _QUERY_MAX_VER,
    user: kb_auth.KBaseUser=Depends(_AUTH),
) -> CollectionVersions:
    store = _precheck_admin_and_get_storage(r, user, "", "view collection versions")
    versions = await store.get_collection_versions(collection_id, max_ver=max_ver)
    counter = await store.get_current_version(collection_id)
    return CollectionVersions(counter=counter, data=versions)


# TODO ROUTES add a admin route to get matches without updating timestamps etc.
#             for now just use the ArangoDB UI or API.


@ROUTER_COLLECTIONS_ADMIN.get(
    "/config/",
    response_model=models.DynamicConfig,
    description="Get the service dynamic configuration. Currently this can be edited only "
        + "directly in the database."
)
async def get_config(r: Request, user: kb_auth.KBaseUser=Depends(_AUTH)) -> models.DynamicConfig:
    _ensure_admin(user, "Only collections service admins can view the service dynamic config")
    return await app_state.get_app_state(r).dyncfgman.get_config()


@ROUTER_DANGER.delete(
    "/admin/matches/{match_id}/",
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
    return await processing_matches.delete_match(appstate, match_id, verbose)


@ROUTER_DANGER.delete(
    "/admin/selections/{selection_id}/",
    response_model=models.SelectionVerbose,
    summary="!!! Danger !!! Delete a selection",
    description="Delete a selection, regardless of state. **BE SURE YOU KNOW WHAT YOU'RE DOING**. "
        + "Deleting a selection when selection processes are running can leave the database in an "
        + "inconsistent state and cause user errors or corrupted results. Even if processes are "
        + "not running, a recent request by a user can result in an error or corrupted results "
        + "if a selection deletion occurs at the same time.",
)
async def delete_selection(
    r: Request,
    selection_id: str = _PATH_SELECTION_ID,
    verbose: bool = _QUERY_SELECTION_VERBOSE,
    user: kb_auth.KBaseUser = Depends(_AUTH),
) -> models.SelectionVerbose:
    _ensure_admin(user, "Only collections service admins can delete selections")
    appstate = app_state.get_app_state(r)
    return await processing_selections.delete_selection(appstate, selection_id, verbose)


@ROUTER_DANGER.delete(
    "/collections/{collection_id}/versions/num/{ver_num}/matchesandselections",
    response_model=DeletedSubsets,
    summary="!!! Danger !!! Delete matches and selections for a collection",
    description="Delete matches and selections for a particular collection version. "
        + "**BE SURE YOU KNOW WHAT YOU'RE DOING**. "
        + "Deleting matches and selections when the processes are running can leave the "
        + "database in an inconsistent state and cause user errors or corrupted results. "
        + "Even if processes are not running, a recent request by a user can result in "
        + "an error or corrupted results if a deletion occurs at the same time."
)
async def delete_matches_and_selections(
    r: Request,
    collection_id: str = PATH_VALIDATOR_COLLECTION_ID,
    ver_num: int = _PATH_VER_NUM,
    force: Annotated[bool, Query(
        example=False,
        description="Delete matches and selections in the processing state as well; "
            + "by default matches and selections in the processing state are ignored. "
            + "This is very dangerous indeed, young hobbit."
        )
    ] = False,
    user: kb_auth.KBaseUser=Depends(_AUTH),
) -> DeletedSubsets:
    _ensure_admin(user, "Only collections service admins can delete matches and selections")
    appstate = app_state.get_app_state(r)
    collspec = models.CollectionSpec(collection_id=collection_id, collection_ver=ver_num)
    async with asyncio.TaskGroup() as tg:
        mtask = tg.create_task(processing_matches.delete_matches_from_collection(
            appstate, collspec, force))
        stask = tg.create_task(processing_selections.delete_selections_from_collection(
            appstate, collspec, force))
    return DeletedSubsets(match_ids=mtask.result(), selection_ids=stask.result())
