"""
Common code for processing matches.
"""

import asyncio
import multiprocessing

from pydantic import BaseModel, Field
from typing import Callable, Any
from src.service import errors
from src.service import models
from src.service.app_state import PickleableDependencies
from src.service.models import remove_non_model_fields
from src.service.storage_arango import ArangoStorage
from src.service.timestamp import now_epoch_millis
from src.service.workspace_wrapper import WorkspaceWrapper


# might want to make this configurable
_PERM_RECHECK_LIMIT = 5 * 60 * 1000  # check perms every 5 mins


class MatchProcess(BaseModel):  #TODO SELECTION can probably rename & reuse this for selections
    """
    Defines a match process.
    """
    process: Callable[[str, PickleableDependencies, list[Any]], None] = Field(
        description="An async callable that processes a match. Takes the match ID, "
            + "the storage system, and the arguments for the match as the callable arguments."
    )
    args: list[Any] = Field(
        description="The arguments for the match."
    )

    def start(self, match_id: str, storage: PickleableDependencies):
        """
        Start the match process in a forkserver.

        match_id - the ID of the match.
        storage - the storage system containing the match and the data to match against.
        """
        ctx = multiprocessing.get_context("forkserver")
        ctx.Process(target=_run_match, args=(self.process, match_id, storage, self.args)).start()


def _run_match(
    function: Callable[[str, PickleableDependencies, list[Any]], None],
    match_id: str,
    pstorage: PickleableDependencies,
    args: list[Any]
):
    asyncio.run(function(match_id, pstorage, args))


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
        match = models.MatchVerbose.construct(**remove_non_model_fields(
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
