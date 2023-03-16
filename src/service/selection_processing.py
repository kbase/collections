"""
Tools for creating and getting selections and starting and recovering selection processes.
"""

import uuid

from src.service.app_state_data_structures import CollectionsState
from src.service import errors
from src.service import models
from src.service import tokens


MAX_SELECTION_IDS = 10000


async def save_selection(
    appstate: CollectionsState,
    coll: models.SavedCollection,
    token: str,
    selection_ids: list[str],
    active_selection_id: str | None = None,
    overwrite: bool = False
):
    """
    Save a selection to the service database.

    appstate - the application state, including the database where the selection will be saved.
    coll - the collection the selection applies to.
    token - the token that serves as the external selection ID.
    selection_ids - the IDs of the data that is selected.
    active_selection_id - the ID of the active selection. This should be unchanged when the
        token for the collection is unchanged. If absent (e.g. a new selection with a new token
        is created), a random ID will be generated.
    overwrite - True to overwrite any existing selection
    """
    if len(selection_ids) > MAX_SELECTION_IDS:
        raise errors.IllegalParameterError(f"At most {MAX_SELECTION_IDS} can be submitted")
    if not coll.default_select:
        raise errors.IllegalParameterError(
            f"Collection {coll.id} version {coll.ver_num} is not configured to allow selections")
    if not active_selection_id:
        # should probably add a uuid method to appstate so these can be mocked
        active_selection_id = str(uuid.uuid4())
    internal_id = str(uuid.uuid4())
    now = appstate.get_epoch_ms()
    internal_sel = models.InternalSelection(
        internal_selection_id=internal_id,
        collection_id=coll.id,
        collection_ver=coll.ver_num,
        data_product = coll.default_select,
        selection_ids=selection_ids,
        unmatched_ids=None,
        created=now,
        heartbeat=None,
        state=models.ProcessState.PROCESSING,
        state_updated=now,
    )
    active_sel = models.ActiveSelection(
        selection_id_hash=tokens.hash_token(token),
        active_selection_id=active_selection_id,
        internal_selection_id=internal_id,
        last_access=now,
    )
    await appstate.arangostorage.save_selection_internal(internal_sel)
    await appstate.arangostorage.save_selection_active(active_sel, overwrite=overwrite)
    # TODO SELECTION start selection process


async def get_internal_selection(
    appstate: CollectionsState,
    token: str,
    verbose: bool = False,
) -> models.InternalSelection:
    hashed_token = tokens.hash_token(token)
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
    return internal_sel
