"""
Common code for processing matches.
"""

import multiprocessing

from pydantic import BaseModel, Field
from typing import Callable, Any
from src.service import models
from src.service.app_state import PickleableDependencies
from src.service.models import remove_non_model_fields
from src.service.storage_arango import ArangoStorage
from src.service.timestamp import now_epoch_millis
from src.service.workspace_wrapper import WorkspaceWrapper


# might want to make this configurable
_PERM_RECHECK_LIMIT = 5 * 60 * 1000  # check perms every 5 mins


class MatchProcess(BaseModel):
    """
    Defines a match process.
    """
    process: Callable[[str, PickleableDependencies, list[Any]], None] = Field(
        description="A callable that processes a match. Takes the match ID, the storage system, "
            + "and the arguments for the match as the callable arguments."
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
        ctx.Process(target=self.process, args=(match_id, storage, self.args)).start()


async def get_match(
    match_id: str,
    username: str,
    storage: ArangoStorage,
    ww: WorkspaceWrapper,
    verbose: bool = False
) -> models.MatchVerbose:
    """
    Get a match by its ID, checking user permissions if necessary.

    match_id - the ID of the match to get.
    username - the name of the user getting the match
    storage - the storage system containing the match.
    ww - a workspace wrapper configured with the user's credentials.
    verbose - True to return the match UPAs and matching IDs. False (the default) to leave them
        empty.
    """
    # TODO MATCHERS add ability to check for state & throw error if not completed
    # TODO MATCHERS add abiltiy to throw error if match is against old version of collection or
    #   wrong collection
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
        await storage.update_match_permissions_check(match_id, username, now)
    else:
        await storage.update_match_last_access(match_id, now)
    match = models.MatchVerbose.construct(**remove_non_model_fields(
        match.dict(), models.MatchVerbose
    ))
    if not verbose:
        match.upas = []
        match.matches = []
    return match
