"""
Data product specific match and selection processing methods.
"""

from functools import partial

from src.service import kb_auth
from src.service import models
from src.service import processing_matches
from src.service import processing_selections
from src.service.app_state_data_structures import PickleableDependencies, CollectionsState
from src.service.data_products.common_functions import (
    override_load_version,
    get_load_version,
    mark_data_by_kbase_id,
)
from src.service.storage_arango import ArangoStorage


MATCH_ID_PREFIX = "m_"
"""
The prefix to apply to matches when storing them in the database to distinguish them
from selections when stored in the same list.
"""

SELECTION_ID_PREFIX = "s_"
"""
The prefix to apply to selections when storing them in the database to distinguish them
from matches when stored in the same list.
"""


async def get_load_version_and_processes( # pretty huge method sig here
    appstate: CollectionsState,
    user: kb_auth.KBaseUser | None,
    collection: str,
    collection_id: str,
    data_product: str,
    load_ver_override: str | None = None,
    match_id: str | None = None,
    selection_id: str | None = None,
) -> tuple[str, models.DataProductProcess, models.DataProductProcess]:
    """
    Get the appropriate load version to use when querying data along with match and / or selection
    processes, if match and selection IDs are provided.

    NOTE: This method only works for applying *already complete* matches and selections to
    data products where the kbase_id field in the data is the match / selection target.

    appstate - the state of the collections service.
    user - the user requesting the information. Required if a load version override is supplied
        (in which case the user must be a collections service administrator) or a match ID is
        supplied.
    collection - the ArangoDB collection containing the data to be matched against. The IDs in the
        *already completed* match and / or selections will be queried against the `kbase_id` field.
    collection_id - the ID of the active collection (e.g. PMI, GTDB, etc.) to query against.
    data_product - the data product to query against.
    load_ver_override - an override for the load version, which is normally retrieved from the
        active collection. In this case the load version from the active collection is ignored.
        If a match or selection ID is provided the load version override is ignored.
    match_id - the match ID to query against. If the match has not already been applied to the
        data product / collection_id combination, a new process will be started to to so.
        Otherwise, the process state will be returned in the 2nd item in the tuple.
    selection_id - the selection ID to query against. If the selection has not already been
        applied to the data product / collection_id combination, a new process will be started
        to to so. Otherwise, the process state will be returned in the 3rd item in the tuple.
    """
    # This method is getting pretty stupidly complex. Not quite sure how to deal with it
    # right now.

    # this is very similar to code in taxa_counts - maybe once it gets cleaned up a bit
    # and handles the dependency on genome_attribs in a saner way it can use this code.
    # Would need to be able to specify its own subsetting fn though
    dp_match, dp_sel = None, None
    lvo = override_load_version(load_ver_override, match_id, selection_id)
    coll, load_ver = await get_load_version(
        appstate.arangostorage, collection_id, data_product, lvo, user)
    if match_id:
        dp_match = await processing_matches.get_or_create_data_product_match_process(
            appstate, coll, user, match_id, data_product,
            partial(_process_subset, collection)
        )
    if selection_id:
        dp_sel = await processing_selections.get_or_create_data_product_selection_process(
            appstate, coll, selection_id, data_product,
            partial(_process_subset, collection)
        )
    return load_ver, dp_match, dp_sel


async def _process_subset(
    collection: str,
    deps: PickleableDependencies,
    storage: ArangoStorage,
    match_or_sel: models.InternalMatch | models.InternalSelection,
    coll: models.SavedCollection,
    dpid: models.DataProductProcessIdentifier,
):
    load_ver = {dp.product: dp.version for dp in coll.data_products}[dpid.data_product]
    missed = await mark_data_by_kbase_id(
        storage,
        collection,
        coll.id,
        load_ver,
        match_or_sel.matches if dpid.is_match() else match_or_sel.selection_ids,
        (MATCH_ID_PREFIX if dpid.is_match() else SELECTION_ID_PREFIX) + dpid.internal_id,
    )
    await storage.update_data_product_process_state(
        dpid, models.ProcessState.COMPLETE, deps.get_epoch_ms(), missing_ids=missed
    )