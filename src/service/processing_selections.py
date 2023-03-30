"""
Tools for creating and getting selections and starting and recovering selection processes.
"""

import hashlib
import logging
import uuid

from typing import Any

from src.service.app_state_data_structures import CollectionsState, PickleableDependencies
from src.service import data_product_specs
from src.service import deletion
from src.service import errors
from src.service import models
from src.service import processing


MAX_SELECTION_IDS = 10000

_UTF_8 = "utf-8"


async def _selection_process(
    selection_id: str,
    deps: PickleableDependencies,
    args: list[Any]
):
    hb = None
    arangoclient = None
    try:
        arangoclient, storage = await deps.get_storage()
        async def heartbeat(millis: int):
            await storage.send_selection_heartbeat(selection_id, millis)
        hb = processing.Heartbeat(heartbeat, processing.HEARTBEAT_INTERVAL_SEC)
        hb.start()
        internal_sel = await storage.get_selection_full(selection_id)
        # throws an error if the data product doesn't exist
        # may want to add a wrapper method to PickleableDependencies so this can be mocked
        data_product = data_product_specs.get_data_product_spec(internal_sel.data_product)
        await data_product.apply_selection(storage, internal_sel.selection_id)
    except Exception as e:
        logging.getLogger(__name__).exception(
            f"Selection process for selection {selection_id} failed")
        await storage.update_selection_state(
            selection_id, models.ProcessState.FAILED, deps.get_epoch_ms())
    finally:
        if hb:
            hb.stop()
        if arangoclient:
            await arangoclient.close()


def _start_process(selection_id: str, appstate: CollectionsState):
    processing.CollectionProcess(process=_selection_process, args=[]
        ).start(selection_id, appstate.get_pickleable_dependencies())


async def save_selection(
    appstate: CollectionsState,
    collection_id: str,
    selection_ids: list[str]
):
    """
    Save a selection to the service database and start the process to apply the selection to
    the collection data.

    appstate - the application state, including the database where the selection will be saved.
    collection_id - the ID of collection the selection applies to.
    selection_ids - the IDs of the data that is selected.
    """
    if len(selection_ids) > MAX_SELECTION_IDS:
        raise errors.IllegalParameterError(f"At most {MAX_SELECTION_IDS} can be submitted")
    coll = await appstate.arangostorage.get_collection_active(collection_id)
    if not coll.default_select:
        raise errors.IllegalParameterError(
            f"Collection {coll.id} version {coll.ver_num} is not configured to allow selections")
    selection_ids = sorted(set(selection_ids))  # remove duplicates
    now = appstate.get_epoch_ms()
    int_sel = models.InternalSelection(
        selection_id=_calc_selection_md5(coll, selection_ids),
        collection_id=coll.id,
        collection_ver=coll.ver_num,
        data_product=coll.default_select,
        selection_ids=selection_ids,
        created=now,
        state=models.ProcessState.PROCESSING,
        state_updated=now,
        # should probably add a uuid method to appstate so these can be mocked
        internal_selection_id=str(uuid.uuid4()),
        last_access=now,
    )
    curr_sel, exists = await appstate.arangostorage.save_selection(int_sel)
    if not exists:
        _start_process(int_sel.selection_id, appstate)
    return curr_sel


# assumes selection_ids are sorted
def _calc_selection_md5(coll: models.SavedCollection, selection_ids: list[str]) -> str:
    # this would be better if it just happened automatically when constructing the pydantic
    # match object, but that doesn't seem to work well with pydantic
    pipe = "|".encode(_UTF_8)
    m = hashlib.md5()
    for var in [coll.id, str(coll.ver_num)] + selection_ids:
        m.update(var.encode(_UTF_8))
        m.update(pipe)
    return m.hexdigest()


async def get_selection(
    appstate: CollectionsState,
    selection_id: str,
    verbose: bool = False,
    require_complete: bool = False,
    require_collection: models.SavedCollection = None,
) -> models.SelectionVerbose:
    """
    Get a selection.

    appstate - the application state.
    selection_id - the ID for the selection.
    verbose - True to return the selection IDs, which may be large compared to the rest of the
        selection
    require_complete - If True, throw an error if the selection process is not yet complete.
    require_collection - require the selection is bound to the given collection.
    """
    return await _get_selection(
        False, appstate, selection_id, verbose, require_complete, require_collection)


async def get_selection_full(
    appstate: CollectionsState,
    selection_id: str,
    verbose: bool = False,
    require_complete: bool = False,
    require_collection: models.SavedCollection = None,
) -> models.InternalSelection:
    """
    As `get_selection` but returns the full internal selection data.
    """
    return await _get_selection(
        True, appstate, selection_id, verbose, require_complete, require_collection)

async def _get_selection(
    internal: bool,
    appstate: CollectionsState,
    selection_id: str,
    verbose: bool,
    require_complete: bool,
    require_collection: models.SavedCollection,
) -> models.SelectionVerbose | models.InternalSelection:
    # could save bandwidth by passing verbose to DB layer and not pulling IDs
    internal_sel = await appstate.arangostorage.get_selection_full(selection_id)
    _check_selection_state(
        appstate,
        internal_sel,
        require_complete,
        require_collection
    )
    await appstate.arangostorage.update_selection_last_access(
        selection_id, appstate.get_epoch_ms())
    if not internal:
        internal_sel = models.SelectionVerbose.construct(**models.remove_non_model_fields(
            internal_sel.dict(), models.SelectionVerbose
        ))
    if not verbose:
        internal_sel.selection_ids = []
        internal_sel.unmatched_ids = None if internal_sel.unmatched_ids is None else []
    return internal_sel


def _check_selection_state(
    appstate: CollectionsState,
    internal_sel: models.InternalSelection,
    require_complete: bool,
    require_collection: models.SavedCollection,
):
    # Code is similar to code in processing_matches.py, but trying to DRY it up was a mess
    col = require_collection
    if col:
        if col.id != internal_sel.collection_id:
            raise errors.InvalidSelectionStateError(
                f"Selection {internal_sel.selection_id} is for collection "
                + f"{internal_sel.collection_id}, not {col.id}")
        if col.ver_num != internal_sel.collection_ver:
            raise errors.InvalidSelectionStateError(
                f"Selection {internal_sel.selection_id} is for collection version "
                + f"{internal_sel.collection_ver}, while the current version is {col.ver_num}")
    # Don't restart the selection if the collection is out of date
    # Also only restart if the selection is requested for the correct collection
    if processing.requires_restart(appstate.get_epoch_ms(), internal_sel):
        logging.getLogger(__name__).warn(
            f"Restarting selection process for ID {internal_sel.selection_id}")
        _start_process(internal_sel.selection_id, appstate)
    # might need to separate out the still processing error from the id / ver matching
    if require_complete and internal_sel.state != models.ProcessState.COMPLETE:
        raise errors.InvalidSelectionStateError(
            f"Selection {internal_sel.selection_id} processing is not complete")


async def get_exportable_types(appstate: CollectionsState, selection_id: str) -> list[str]:
    """
    Get the Workspace types for the data that is exportable in a selection.

    appstate - the application state
    selection_id - the selection of interest
    """
    # We don't know the collection ID yet so we can't require a collection. Don't bother
    # with requiring complete either.
    sel = await get_selection_full(appstate, selection_id)
    storage = appstate.arangostorage
    # If we do want to check the collection version, we need to pull the active collection
    coll = await storage.get_collection_version_by_num(sel.collection_id, sel.collection_ver)
    # sel.data_product is checked against the collection when creating the selection so it must
    # be present
    load_ver = {dp.product: dp.version for dp in coll.data_products}[sel.data_product]
    return await storage.get_export_types(coll.id, sel.data_product, load_ver)


async def delete_selection(appstate: CollectionsState, selection_id: str, verbose: bool = False
) -> models.SelectionVerbose:
    """
    Move a selection record to the deleted state, awaiting permanent deletion.

    appstate - the application state.
    selection_id - the selection to delete.
    verbose - True to return the selection IDs and unmatched IDs, which may be much larger than
        the rest of the selection data.
    """
    store = appstate.arangostorage
    sel = await store.get_selection_full(selection_id)
    await deletion.move_selection_to_deleted_state(store, sel, appstate.get_epoch_ms())
    sel = models.SelectionVerbose(
        **models.remove_non_model_fields(sel.dict(), models.SelectionVerbose))
    if not verbose:
        # TODO PERF do this by not requesting the fields from the DB
        sel.selection_ids = []
        sel.unmatched_ids = None if sel.unmatched_ids is None else []
    return sel
