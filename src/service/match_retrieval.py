"""
Methods for retriving matches from a storage system, ensuring that the user has permissions
to the match, the match is in the expected state, and access times are updated correctly.
"""

import logging
from typing import Any, Callable

from src.service import app_state
from src.service import errors
from src.service import models
from src.service import processing
from src.service.storage_arango import ArangoStorage
from src.service.timestamp import now_epoch_millis
from src.service.workspace_wrapper import WorkspaceWrapper


# might want to make this configurable
_PERM_RECHECK_LIMIT = 5 * 60 * 1000  # check perms every 5 mins


async def get_match(
    match_id: str,
    username: str,
    storage: ArangoStorage,
    ww: WorkspaceWrapper,
    verbose: bool = False,
    require_complete: bool = False,
    require_collection: models.SavedCollection = None,
) -> models.MatchVerbose:
    """
    Get a match by its ID, checking user permissions for the workspaces in the match if necessary.
    Note that the deletion state of objects is not checked.

    match_id - the ID of the match to get.
    username - the name of the user getting the match
    storage - the storage system containing the match.
    ww - a workspace wrapper configured with the user's credentials.
    verbose - True to return the match UPAs and matching IDs. False (the default) to leave them
        empty.
    require_complete - require that the match is in the "complete" state.
    require_collection - require that the match collection and collection version are the same
        as those in the given collections.
    """
    return await _get_match(
        False,
        match_id,
        username,
        storage,
        ww,
        verbose,
        require_complete,
        require_collection)


async def get_match_full(
    match_id: str,
    username: str,
    storage: ArangoStorage,
    ww: WorkspaceWrapper,
    verbose: bool = False,
    require_complete: bool = False,
    require_collection: models.SavedCollection = None,
) -> models.InternalMatch:
    """
    As `get_match`, but returns an internal match rather than a verbose match.
    """
    return await _get_match(
        True,
        match_id,
        username,
        storage,
        ww,
        verbose,
        require_complete,
        require_collection)


async def _get_match(
    internal: bool,
    match_id: str,
    username: str,
    storage: ArangoStorage,
    ww: WorkspaceWrapper,
    verbose: bool,
    require_complete: bool,
    require_collection: models.SavedCollection,
):
    # could save bandwidth if we added option to not return upas and match IDs if not verbose
    match = await storage.get_match_full(match_id)
    last_perm_check = match.user_last_perm_check.get(username)
    now = now_epoch_millis()
    # if we ever go back in time to 1970 the first part of this clause will be an issue
    if not last_perm_check or now - last_perm_check > _PERM_RECHECK_LIMIT:
        # If we want to be really careful we should recheck all the UPAs again since
        # objects might've been deleted, but this is in most cases way faster and it's not clear
        # if the objects in a match being deleted after the fact is a problem.
        # For now just do it the fast way.
        ww.check_workspace_permissions(set(match.wsids))
        _check_match_state(match, require_complete, require_collection)
        await storage.update_match_permissions_check(match_id, username, now)
    else:
        _check_match_state(match, require_complete, require_collection)
        await storage.update_match_last_access(match_id, now)
    if not internal:
        match = models.MatchVerbose.construct(**models.remove_non_model_fields(
            match.dict(), models.MatchVerbose
        ))
    if not verbose:
        match.upas = []
        match.matches = []
    return match


def _check_match_state(
    match: models.Match,
    require_complete: bool,
    require_collection: models.SavedCollection
) -> None:
    # might need to separate out the still processing error from the id / ver matching
    if require_complete and match.match_state != models.MatchState.COMPLETE:
        raise errors.InvalidMatchState(f"Match {match.match_id} processing is not complete")
    col = require_collection
    if col:
        if col.id != match.collection_id:
            raise errors.InvalidMatchState(
                f"Match {match.match_id} is for collection {match.collection_id}, not {col.id}")
        if col.ver_num != match.collection_ver:
            raise errors.InvalidMatchState(
                f"Match {match.match_id} is for collection version {match.collection_ver}, "
                + f"while the current version is {col.ver_num}")


async def get_or_create_data_product_match(
    storage: ArangoStorage,
    # could theoretically get storage from here, but why instantiate another arango client
    deps: app_state.PickleableDependencies,
    match: models.InternalMatch,
    data_product: str,
    match_process: Callable[[str, app_state.PickleableDependencies, list[Any]], None],
) -> models.DataProductMatchProcess:
    """
    Get a match process data structure for a data product match.

    Creates the match and starts the matching process if the match does not already exist.

    If the match exists but hasn't had a heartbeat in the required time frame, starts another
    instance of the processes.

    In either case, the list of arguments to the match process is empty.

    storage - the storage system holding the match data.
    deps - pickleable dependencies to pass to the match process.
    match - the parent match for the data product match.
    data_product - the data product for the match.
    match_process - the match process to run if there is no match information in the database or
        the match heartbeat is too old.
    """
    now = now_epoch_millis()
    dp_match, exists = await storage.create_or_get_data_product_match(
        models.DataProductMatchProcess(
            data_product=data_product,
            internal_match_id=match.internal_match_id,
            created=now,
            data_product_match_state=models.MatchState.PROCESSING,
            data_product_match_state_updated=now,
        )
    )
    if not exists:
        _start_process(match.match_id, match_process, deps)
    elif dp_match.data_product_match_state == models.MatchState.PROCESSING:
        # "failed" indicates the failure is not necessarily recoverable
        # E.g. an admin should take a look
        # We may need to add another state for recoverable errors like loss of contact w/ arango...
        # but that kind of thing could lead to a lot of jobs being kicked off over and over
        # Better to put retries in the matching code or arango storage wrapper
        maxdiff = processing.HEARTBEAT_RESTART_THRESHOLD_MS
        now = now_epoch_millis()
        if dp_match.heartbeat is None:
            if now - dp_match.created > maxdiff:
                _warn(dp_match.internal_match_id, data_product)    
                _start_process(match.match_id, match_process, deps)
        elif now - dp_match.heartbeat > maxdiff:
            _warn(dp_match.internal_match_id, data_product)    
            _start_process(match.match_id, match_process, deps)
    return dp_match


def _warn(
    internal_match_id: str,
    data_product: str,
) -> None:
    logging.getLogger(__name__).warn(
        f"Restarting match process for match {internal_match_id} "
        + f"data product {data_product}"
    )


def _start_process(
    match_id: str,
    match_process: Callable[[str, app_state.PickleableDependencies, list[Any]], None],
    deps: app_state.PickleableDependencies,
) -> None:
    processing.CollectionProcess(process=match_process, args=[]).start(match_id, deps)
