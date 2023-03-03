"""
Methods for retriving matches from a storage system, ensuring that the user has permissions
to the match, the match is in the expected state, and access times are updated correctly.
"""

import logging
from typing import Any, Callable

from src.service.app_state_data_structures import PickleableDependencies, CollectionsState
# kinda feel like users should be more generic, but not work the trouble
from src.service import kb_auth
from src.service import errors
from src.service import models
from src.service import processing
from src.service.matchers.common_models import Matcher
from src.service.storage_arango import ArangoStorage
from src.service.workspace_wrapper import WorkspaceWrapper


# might want to make this configurable
_PERM_RECHECK_LIMIT = 5 * 60 * 1000  # check perms every 5 mins


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
    ww = WorkspaceWrapper(deps.get_workspace_client(user.token))
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
        ww.check_workspace_permissions(set(match.wsids))  # do this before checking match state
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
            raise errors.InvalidMatchState(
                f"Match {match.match_id} is for collection {match.collection_id}, not {col.id}")
        if col.ver_num != match.collection_ver:
            raise errors.InvalidMatchState(
                f"Match {match.match_id} is for collection version {match.collection_ver}, "
                + f"while the current version is {col.ver_num}")
    # Don't restart the match if the collection is out of date
    # Also only restart if the match is requested for the correct collection
    if _requires_restart(deps, match, match.match_state):
        mp = await create_match_process(
            deps.get_matcher(match.matcher_id), ww, match.upas, match.collection_parameters
        )
        logging.getLogger(__name__).warn(f"Restarting match process for match {match.match_id}")
        mp.start(match.match_id, deps.get_pickleable_dependencies())
    # might need to separate out the still processing error from the id / ver matching
    if require_complete and match.match_state != models.MatchState.COMPLETE:
        raise errors.InvalidMatchState(f"Match {match.match_id} processing is not complete")


def _requires_restart(
    deps: CollectionsState,
    match: models.InternalMatch | models.DataProductMatchProcess,
    match_state: models.MatchState,
) -> bool:
    if match_state == models.MatchState.PROCESSING:
        # "failed" indicates the failure is not necessarily recoverable
        # E.g. an admin should take a look
        # We may need to add another state for recoverable errors like loss of contact w/ arango...
        # but that kind of thing could lead to a lot of jobs being kicked off over and over
        # Better to put retries in the matching code or arango storage wrapper
        maxdiff = processing.HEARTBEAT_RESTART_THRESHOLD_MS
        now = deps.get_epoch_ms()
        if match.heartbeat is None:
            if now - match.created > maxdiff:
                return True
        elif now - match.heartbeat > maxdiff:
            return True
    return False


async def create_match_process(
    matcher: Matcher,
    ww: WorkspaceWrapper,
    upas: list[str],
    # TODO MATCHERS user parameters when needed
    collection_parameters: dict[str, Any],
) -> processing.CollectionProcess:
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
    """
    # All matchers will need to check permissions and deletion state for the workspace objects,
    # so we get the metadata which is the cheapest way to do that. Most matchers will need
    # the metadata anyway.
    # Getting the objects might be really expensive depending on the size and number, so we
    # leave that to the matchers themselves, which should probably start a ee2 (?) job if object
    # downloads are required
    # TODO PERFORMANCE might want to write our own async routines for contacting the workspace
    #      vs using the compiled client. Made this method async just in case
    meta = ww.get_object_metadata(upas, allowed_types=matcher.types)
    return matcher.generate_match_process(
        {u: m for u, m in zip(upas, meta)},
        collection_parameters,
    )


async def get_or_create_data_product_match(
    deps: CollectionsState,
    match: models.InternalMatch,
    data_product: str,
    match_process: Callable[[str, PickleableDependencies, list[Any]], None],
) -> models.DataProductMatchProcess:
    """
    Get a match process data structure for a data product match.

    Creates the match and starts the matching process if the match does not already exist.

    If the match exists but hasn't had a heartbeat in the required time frame, starts another
    instance of the processes.

    In either case, the list of arguments to the match process is empty.

    deps - the collections service state.
    match - the parent match for the data product match.
    data_product - the data product for the match.
    match_process - the match process to run if there is no match information in the database or
        the match heartbeat is too old.
    """
    now = deps.get_epoch_ms()
    dp_match, exists = await deps.arangostorage.create_or_get_data_product_match(
        models.DataProductMatchProcess(
            data_product=data_product,
            internal_match_id=match.internal_match_id,
            created=now,
            data_product_match_state=models.MatchState.PROCESSING,
            data_product_match_state_updated=now,
        )
    )
    if not exists:
        _start_process(match.match_id, match_process, deps.get_pickleable_dependencies())
    elif _requires_restart(deps, dp_match, dp_match.data_product_match_state):
        logging.getLogger(__name__).warn(
            f"Restarting match process for match {dp_match.internal_match_id} "
            + f"data product {data_product}"
        )
        _start_process(match.match_id, match_process, deps.get_pickleable_dependencies())
    return dp_match


def _start_process(
    match_id: str,
    match_process: Callable[[str, PickleableDependencies, list[Any]], None],
    deps: PickleableDependencies,
) -> None:
    processing.CollectionProcess(process=match_process, args=[]).start(match_id, deps)
