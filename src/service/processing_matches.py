"""
Methods for retriving matches from a storage system, ensuring that the user has permissions
to the match, the match is in the expected state, and access times are updated correctly as well
as starting match processes and recovering processes that are stalled when requesting a match.
"""

import hashlib
import json
import jsonschema
import logging
import uuid

from typing import Any, Callable, Awaitable
from collections.abc import Iterable

from src.service.app_state_data_structures import CollectionsState, PickleableDependencies
# kinda feel like users should be more generic, but not work the trouble
from src.service import kb_auth
from src.service import deletion
from src.service import errors
from src.service import models
from src.service import processing
from src.service.matchers.common_models import Matcher
from src.service.storage_arango import ArangoStorage
from src.service.workspace_wrapper import WorkspaceWrapper, WORKSPACE_UPA_PATH


MAX_UPAS = 10000

# might want to make this configurable
_PERM_RECHECK_LIMIT = 5 * 60 * 1000  # check perms every 5 mins

_UTF_8 = "utf-8"


async def create_match(
    appstate: CollectionsState,
    collection_id: str,
    matcher_id: str,
    user: kb_auth.KBaseUser,
    upas: list[str],
    match_params: dict[str, Any],
) -> models.Match:
    coll = await appstate.arangostorage.get_collection_active(collection_id)
    matcher_info = _get_matcher_from_collection(coll, matcher_id)
    ww = WorkspaceWrapper(appstate.sdk_client, token=user.token)
    internal_match_id = str(uuid.uuid4())
    matcher = appstate.get_matcher(matcher_info.matcher)
    match_process, upas, wsids = await _create_match_process(
        internal_match_id,
        matcher,
        ww,
        upas,
        match_params,
        matcher_info.parameters,
    )
    perm_check = appstate.get_epoch_ms()
    params = match_params or {}
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
        internal_match_id=internal_match_id,
        wsids=wsids,
        created=perm_check,
        last_access=perm_check,
        user_last_perm_check={user.user.id: perm_check}
    )
    curr_match, exists = await appstate.arangostorage.save_match(int_match)
    # don't bother checking if the match heartbeat is old here, just do it in the access methods
    if not exists:
        match_process.start(appstate.get_pickleable_dependencies())
    return curr_match


def _get_matcher_from_collection(collection: models.SavedCollection, matcher_id: str
) -> models.Matcher:
    for m in collection.matchers:
        if m.matcher == matcher_id:
            return m
    raise errors.NoRegisteredMatcherError(matcher_id)


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
    pipe = "|".encode(_UTF_8)
    m = hashlib.md5()
    for var in [matcher_id, collection_id, str(collection_ver)]:
        m.update(var.encode(_UTF_8))
        m.update(pipe)
    # this will not sort arrays, and arrays might have positional semantics so we shouldn't do
    # that anyway. If we have match parameters where sorting arrays is an issue we'll need
    # to implement on a per matcher basis.
    m.update(json.dumps(params, sort_keys=True).encode(_UTF_8))
    m.update(pipe)
    for u in upas:
        m.update(u.encode(_UTF_8))
        m.update(pipe)
    return m.hexdigest()


async def get_match(
    deps: CollectionsState,
    match_id: str,
    user: kb_auth.KBaseUser,
    verbose: bool = False,
    require_complete: bool = False,
    require_collection: models.SavedCollection = None,
) -> models.MatchVerbose:
    """
    Get a match by its ID, checking user permissions for the workspaces in the match if necessary.
    Note that the deletion state of objects is not checked.

    If the match process is determined to be dead based on the state of the process and last
    heartbeat, the match process will be restarted.

    deps - collections dependendies.
    match_id - the ID of the match to get.
    user - the user getting the match
    verbose - True to return the match UPAs and matching IDs. False (the default) to leave them
        empty.
    require_complete - require that the match is in the "complete" state.
    require_collection - require that the match collection and collection version are the same
        as those in the given collections.
    """
    return await _get_match(
        False,
        deps,
        match_id,
        user,
        verbose,
        require_complete,
        require_collection)


async def get_match_full(
    deps: CollectionsState,
    match_id: str,
    user: kb_auth.KBaseUser,
    verbose: bool = False,
    require_complete: bool = False,
    require_collection: models.SavedCollection = None,
) -> models.InternalMatch:
    """
    As `get_match`, but returns an internal match rather than a verbose match.
    """
    return await _get_match(
        True,
        deps,
        match_id,
        user,
        verbose,
        require_complete,
        require_collection)


async def _get_match(
    internal: bool,
    deps: CollectionsState,
    match_id: str,
    user: kb_auth.KBaseUser,
    verbose: bool,
    require_complete: bool,
    require_collection: models.SavedCollection,
):
    storage = deps.arangostorage
    ww = WorkspaceWrapper(deps.sdk_client, user.token)
    # could save bandwidth if we added option to not return upas and match IDs if not verbose
    match = await storage.get_match_full(match_id)
    last_perm_check = match.user_last_perm_check.get(user.user.id)
    now = deps.get_epoch_ms()
    # if we ever go back in time to 1970 the first part of this clause will be an issue
    if not last_perm_check or now - last_perm_check > _PERM_RECHECK_LIMIT:
        # If we want to be really careful we should recheck all the UPAs again since
        # objects might've been deleted, but this is in most cases way faster and it's not clear
        # if the objects in a match being deleted after the fact is a problem.
        # For now just do it the fast way.
        # TODO MATCHERS document the above
        await ww.check_workspace_permissions(set(match.wsids))  # do before checking match state
        await _check_match_state(match, require_complete, require_collection, deps, ww)
        await storage.update_match_permissions_check(match_id, user.user.id, now)
    else:
        await _check_match_state(match, require_complete, require_collection, deps, ww)
        await storage.update_match_last_access(match_id, now)
    if not internal:
        match = models.MatchVerbose.construct(**models.remove_non_model_fields(
            match.dict(), models.MatchVerbose
        ))
    if not verbose:
        match.upas = []
        match.matches = []
    return match


async def _check_match_state(
    match: models.MatchVerbose,
    require_complete: bool,
    require_collection: models.SavedCollection,
    deps: CollectionsState,
    ww: WorkspaceWrapper,
) -> None:
    col = require_collection
    if col:
        if col.id != match.collection_id:
            raise errors.InvalidMatchStateError(
                f"Match {match.match_id} is for collection {match.collection_id}, not {col.id}")
        if col.ver_num != match.collection_ver:
            raise errors.InvalidMatchStateError(
                f"Match {match.match_id} is for collection version {match.collection_ver}, "
                + f"while the current version is {col.ver_num}")
    # Don't restart the match if the collection is out of date
    # Also only restart if the match is requested for the correct collection
    if processing.requires_restart(deps.get_epoch_ms(), match):
        mp, _, _ = await _create_match_process(
            match.internal_match_id,
            deps.get_matcher(match.matcher_id),
            ww,
            match.upas,
            match.user_parameters,
            match.collection_parameters
        )
        logging.getLogger(__name__).warn(f"Restarting match process for match {match.match_id}")
        mp.start(deps.get_pickleable_dependencies())
    # might need to separate out the still processing error from the id / ver matching
    if require_complete and match.state != models.ProcessState.COMPLETE:
        raise errors.InvalidMatchStateError(f"Match {match.match_id} processing is not complete")


async def _create_match_process(
    internal_match_id: str,
    matcher: Matcher,
    ww: WorkspaceWrapper,
    upas: list[str],
    user_parameters: dict[str, Any],
    collection_parameters: dict[str, Any],
) -> tuple[processing.CollectionProcess, list[str], set[int]]:
    """
    Create a match process given inputs to the match. The process can be started immediately or
    at a later time (once match information has been saved to a database, for example.)

    Checks that the user has access to all the workspace UPAs in the match, including whether
    they're deleted or not.

    matcher - the matcher to use.
    ww - a workspace wrapper initialized with the credentials of the user
    upas - the UPAs of the workspce objects to include in the match.
    collection_parameters - the parameters for the match provided by the collection data
        (as opposed to user provided parameters.)

    Returns a tuple containing
        1. The match process
        2. The list of UPAs after expanding sets and removing duplicates,
        3. The set of workspace IDs from the root UPAs in the list of UPAs.
    """
    # All matchers will need to check permissions and deletion state for the workspace objects,
    # so we get the metadata which is the cheapest way to do that. Most matchers will need
    # the metadata anyway.
    # Getting the objects might be really expensive depending on the size and number, so we
    # leave that to the matchers themselves, which should probably start a ee2 (?) job if object
    # downloads are required
    upas, _ = _check_and_sort_UPAs_and_get_wsids(upas)
    if user_parameters:
        try:
            jsonschema.validate(instance=user_parameters, schema=matcher.user_parameters)
        except jsonschema.exceptions.ValidationError as e:
            raise errors.IllegalParameterError(
                # TODO MATCHERS str(e) is pretty gnarly. Figure out a nicer representation
                f"Failed to validate user parameters for matcher {matcher.id}: {e}")
    if len(upas) > MAX_UPAS:
        raise errors.IllegalParameterError(f"No more than {MAX_UPAS} UPAs are allowed per match")
    meta = await ww.get_object_metadata(
        upas,
        allowed_types=matcher.types,
        allowed_set_types=matcher.set_types
    )
    upa2meta = {m[WORKSPACE_UPA_PATH]: m for m in meta}
    upas, wsids = _check_and_sort_UPAs_and_get_wsids(upa2meta.keys())
    if len(upas) > MAX_UPAS:
        raise errors.IllegalParameterError(f"No more than {MAX_UPAS} are allowed per match - "
            + "this limit was violated after set expansion")
    upa2meta = {u: upa2meta[u] for u in upas}
    return matcher.generate_match_process(
        internal_match_id, upa2meta, user_parameters, collection_parameters
    ), upas, wsids


def _check_and_sort_UPAs_and_get_wsids(upas: Iterable[str]) -> tuple[list[str], set[int]]:
    # We check UPA format prior to sending them to the workspace since figuring out the
    # type of the WS error is too much of a pain. Easier to figure out obvious errors first
    
    # Probably a way to make this faster / more compact w/ list comprehensions, but not worth
    # the time I was putting into it

    # Removes upa paths that point at the same object, preferentially taking the first shortest
    # path
    upa_to_path = {}
    for i, upapath in enumerate(upas):
        upapath_split = upapath.strip().split(";")
        # deal with trailing ';'
        upapath_split = upapath_split[:-1] if not upapath_split[-1] else upapath_split
        if not upapath_split:  # ignore empty lines
            continue
        upapath_parsed = []
        for upa in upapath_split:
            upaparts = upa.split("/")
            if len(upaparts) != 3:
                _upaerror(upa, upapath, len(upapath_split), i)
            try:
                upapath_parsed.append(tuple(int(part) for part in upaparts))
            except ValueError:
                _upaerror(upa, upapath, len(upapath_split), i)
            if any([n < 1 for n in upapath_parsed[-1]]):
                # might want to have a more specific error here, meh for now
                _upaerror(upa, upapath, len(upapath_split), i)
        target_object = upapath_parsed[-1]
        if target_object in upa_to_path:
            if len(upapath_parsed) < len(upa_to_path[target_object]):
                # if there are multiple paths to the same object, use the shortest
                upa_to_path[target_object] = upapath_parsed
        else:
            upa_to_path[target_object] = upapath_parsed
    upas_parsed = sorted(upa_to_path.values())
    wsids = {arr[0][0] for arr in upas_parsed}
    ret = []
    for path in upas_parsed:
        path_list = []
        for upa in path:
            path_list.append("/".join([str(x) for x in upa]))
        ret.append(";".join(path_list))
    return ret, wsids


def _upaerror(upa, upapath, upapathlen, index):
    if upapathlen > 1:
        raise errors.IllegalParameterError(
            f"Illegal UPA '{upa}' in path '{upapath}' at index {index}")
    else:
        raise errors.IllegalParameterError(f"Illegal UPA '{upa}' at index {index}")


async def delete_match(appstate: CollectionsState, match_id: str, verbose: bool = False
) -> models.MatchVerbose:
    """
    Move a match record to the deleted state, awaiting permanent deletion.

    appstate - the application state.
    match_id - the match to delete.
    verbose - True to return the match UPAs and matches, which may be much larger than the rest
        of the match data.
    """
    store = appstate.arangostorage
    match = await store.get_match_full(match_id)
    await deletion.move_match_to_deleted_state(store, match, appstate.get_epoch_ms())
    match = models.MatchVerbose(
        **models.remove_non_model_fields(match.dict(), models.MatchVerbose))
    if not verbose:
        # TODO PERF do this by not requesting the fields from the DB
        match.upas = []
        match.matches = []
    return match


async def delete_matches_from_collection(
    appstate: CollectionsState, collspec: models.CollectionSpec, force: bool = False
) -> list[str]:
    """
    Moves match records for a particular collection version to the deleted state if they're no 
    longer processing and returns the IDs of the matches.

    appstate - the application state.
    collspec - the collection to process.
    force - remove matches that are in the processing state as well, although this is not
        advised and may cause unexpected behavior
    """
    store = appstate.arangostorage
    ids = []
    async def _proc(match: models.InternalMatch):
        ids.append(match.match_id)
        await deletion.move_match_to_deleted_state(store, match, appstate.get_epoch_ms())
    states = [] if force else set(models.ProcessState) - set([models.ProcessState.PROCESSING])
    await store.process_collection_matches(collspec, _proc, states=states)
    return ids


async def get_or_create_data_product_match_process(
    appstate: CollectionsState,
    coll: models.SavedCollection,
    user: kb_auth.KBaseUser,
    match_id: str,
    data_product: str,
    match_fn: Callable[
        [
            PickleableDependencies,
            ArangoStorage,
            models.InternalMatch | models.InternalSelection,
            models.SavedCollection,
            models.DataProductProcessIdentifier
        ],
        Awaitable[None],
    ],
) -> models.DataProductProcess:
    """
    Get a process data structure for a match data product process.

    Creates and starts the process if the process does not already exist.

    If the process exists but hasn't had a heartbeat in the required time frame, starts another
    instance of the process.

    appstate - the collections service state.
    coll - the most recent version of the collection associated with the match.
    user - the user performing the match.
    match_id - the ID of the match.
    data_product - the data product performing the match.
    match_fn - the function that will perform the match calculations and DB updates.
        Runs if there is no process information in the database or
        the heartbeat is too old. The arguments are
            * the system dependencies (provided by `appstate`),
            * a storage system created from the system dependencies. The storage system will
              be closed by the calling function after the callable returns
            * the match or selection
            * the collection (pulled from the storage system via the data in the subset record)
            * the data product process identifier.
    """
    if not user:
        raise errors.UnauthorizedError("Authentication is required if a match ID is supplied")
    match = await get_match_full(
            appstate, match_id, user, require_complete=True, require_collection=coll)
    dpid = models.DataProductProcessIdentifier(
        internal_id=match.internal_match_id,
        data_product=data_product,
        type=models.SubsetType.MATCH,
    )
    return await processing.get_or_create_data_product_process(appstate, dpid, match_fn)
