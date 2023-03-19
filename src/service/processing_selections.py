"""
Tools for creating and getting selections and starting and recovering selection processes.
"""

import logging
import uuid

from typing import Any

from src.service.app_state_data_structures import CollectionsState, PickleableDependencies
from src.service import data_product_specs
from src.service import errors
from src.service import models
from src.service import processing
from src.service import tokens


MAX_SELECTION_IDS = 10000


async def _selection_process(
    internal_selection_id: str,
    deps: PickleableDependencies,
    args: list[Any]
):
    hb = None
    arangoclient = None
    try:
        arangoclient, storage = await deps.get_storage()
        async def heartbeat(millis: int):
            await storage.send_selection_heartbeat(internal_selection_id, millis)
        hb = processing.Heartbeat(heartbeat, processing.HEARTBEAT_INTERVAL_SEC)
        hb.start()
        internal_sel = await storage.get_selection_internal(internal_selection_id)
        # throws an error if the data product doesn't exist
        # may want to add a wrapper method to PickleableDependencies so this can be mocked
        data_product = data_product_specs.get_data_product_spec(internal_sel.data_product)
        await data_product.apply_selection(storage, internal_selection_id)
    except Exception as e:
        logging.getLogger(__name__).exception(
            f"Selection process for internal selection {internal_selection_id} failed")
        await storage.update_selection_state(
            internal_selection_id, models.ProcessState.FAILED, deps.get_epoch_ms())
    finally:
        if hb:
            hb.stop()
        if arangoclient:
            await arangoclient.close()


def _start_process(internal_selection_id: str, appstate: CollectionsState):
    # Theoretically we could save some CPU by finding selecions with the same selection list
    # and reusing the results, but given they're isolated by token that seems like a pain
    # YAGNI until it becomes an issue
    processing.CollectionProcess(process=_selection_process, args=[]
        ).start(internal_selection_id, appstate.get_pickleable_dependencies())


async def save_selection(
    appstate: CollectionsState,
    coll: models.SavedCollection,
    token: str,
    selection_ids: list[str],
    active_selection_id: str | None = None,
    overwrite: bool = False
):
    """
    Save a selection to the service database and start the process to apply the selection to
    the collection data.

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
    _start_process(internal_id, appstate)


async def get_selection(
    appstate: CollectionsState,
    token: str,
    verbose: bool = False,
    require_complete: bool = False,
    require_collection: models.SavedCollection = None,
) -> tuple[models.ActiveSelection, models.InternalSelection]:
    """
    Get a selection.

    appstate - the application state.
    token - the token for the selection.
    verbose - True to return the selection IDs, which may be large compared to the rest of the
        selection
    require_complete - If True, throw an error if the selection process is not yet complete.
    require_collection - require the selection is bound to the given collection.

    Returns the active selection for the token and its corresponding internal selection.
    """
    hashed_token = tokens.hash_token(token)
    active_sel = await appstate.arangostorage.get_selection_active(hashed_token)
    # could save bandwidth by passing verbose to DB layer and not pulling IDs
    internal_sel = await appstate.arangostorage.get_selection_internal(
        active_sel.internal_selection_id)
    _check_selection_state(
        appstate,
        active_sel.active_selection_id,
        internal_sel,
        require_complete,
        require_collection
    )
    await appstate.arangostorage.update_selection_active_last_access(
        hashed_token, appstate.get_epoch_ms())
    if not verbose:
        internal_sel.selection_ids = []
        internal_sel.unmatched_ids = None if internal_sel.unmatched_ids is None else []
    return active_sel, internal_sel


def _check_selection_state(
    appstate: CollectionsState,
    active_sel_id: str,
    internal_sel: models.InternalSelection,
    require_complete: bool,
    require_collection: models.SavedCollection,
):
    # Code is similar to code in processing_matches.py, but trying to DRY it up was a mess
    # We DEF don't want to put the selection token in the error
    col = require_collection
    if col:
        if col.id != internal_sel.collection_id:
            raise errors.InvalidSelectionStateError(
                f"Selection {active_sel_id} is for collection "
                + f"{internal_sel.collection_id}, not {col.id}")
        if col.ver_num != internal_sel.collection_ver:
            raise errors.InvalidSelectionStateError(
                f"Selection {active_sel_id} is for collection version "
                + f"{internal_sel.collection_ver}, while the current version is {col.ver_num}")
    # Don't restart the selection if the collection is out of date
    # Also only restart if the selection is requested for the correct collection
    if processing.requires_restart(appstate.get_epoch_ms(), internal_sel):
        logging.getLogger(__name__).warn(
            f"Restarting selection process for internal ID {internal_sel.internal_selection_id}")
        _start_process(internal_sel.internal_selection_id, appstate)
    # might need to separate out the still processing error from the id / ver matching
    if require_complete and internal_sel.state != models.ProcessState.COMPLETE:
        raise errors.InvalidSelectionStateError(
            f"Selection {active_sel_id} processing is not complete")
