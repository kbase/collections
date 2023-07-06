"""
The samples data product, which provides sample information for a collection.
"""

from collections import defaultdict
import logging

from fastapi import APIRouter, Request, Depends, Query
from pydantic import BaseModel, Field, Extra
import src.common.storage.collection_and_field_names as names
from src.common.product_models.common_models import SubsetProcessStates
from src.service import app_state
from src.service.app_state_data_structures import CollectionsState, PickleableDependencies
from src.service import errors
from src.service import kb_auth
from src.service import processing_matches
from src.service import models
from src.service import processing_selections
from src.service.data_products.common_functions import (
    get_load_version,
    remove_collection_keys,
    query_simple_collection_list,
    count_simple_collection_list,
    mark_data_by_kbase_id,
    remove_marked_subset,
    override_load_version,
)
from src.service.data_products import common_models
from src.service.data_products.data_product_processing import (
    MATCH_ID_PREFIX,
    SELECTION_ID_PREFIX,
    get_load_version_and_processes,
)
from src.service.data_products.table_models import TableAttributes
from src.service.http_bearer import KBaseHTTPBearer
from src.service.routes_common import PATH_VALIDATOR_COLLECTION_ID
from src.service.storage_arango import ArangoStorage, remove_arango_keys
from src.service.timestamp import now_epoch_millis
from typing import Any, Callable, Annotated

# Implementation note - we know FLD_KBASE_ID is unique per collection id /
# load version combination since the loader uses those 3 fields as the arango _key

# Implementation note 2 - we were directed to make a flat record, e.g. 1 sample record for each
# kbase_id, even though there are many kbase_ids per sample. This will probably need to be
# reworked later to remove the many duplicate sample records and instead have some kind of
# M:1 kbase_id:sample relationship.

ID = "samples"

_ROUTER = APIRouter(tags=["Samples"], prefix=f"/{ID}")


class SamplesSpec(common_models.DataProductSpec):

    async def delete_match(self, storage: ArangoStorage, internal_match_id: str):
        """
        Delete sample match data.

        storage - the storage system
        internal_match_id - the match to delete.
        """
        await remove_marked_subset(
            storage, names.COLL_SAMPLES, MATCH_ID_PREFIX + internal_match_id)

    async def delete_selection(self, storage: ArangoStorage, internal_selection_id: str):
        """
        Delete sample selection data.

        storage - the storage system
        internal_selection_id - the selection to delete.
        """
        await remove_marked_subset(
            storage, names.COLL_SAMPLES, SELECTION_ID_PREFIX + internal_selection_id)


SAMPLES_SPEC = SamplesSpec(
    data_product=ID,
    router=_ROUTER,
    db_collections=[
        common_models.DBCollection(
            name=names.COLL_SAMPLES,
            indexes=[
                [
                    names.FLD_COLLECTION_ID,
                    names.FLD_LOAD_VERSION,
                    names.FLD_KBASE_ID,
                    # Since this is the default sort option (see below), we specify an index
                    # for fast sorts since every time the user hits the UI for the first time
                    # or without specifying a sort order it'll sort on this field
                ],
                [
                    names.FLD_COLLECTION_ID,
                    names.FLD_LOAD_VERSION,
                    names.FLD_SAMPLE_LONGITUDE,
                    names.FLD_SAMPLE_LATITUDE,
                    # for aggregating on location
                ],
                [
                    names.FLD_COLLECTION_ID,
                    names.FLD_LOAD_VERSION,
                    # https://www.arangodb.com/docs/stable/indexing-index-basics.html#indexing-array-values
                    names.FLD_MATCHES_SELECTIONS + "[*]",
                    names.FLD_KBASE_ID,
                    # for finding matches/selections, and opt a default sort on the kbase ID
                ],
                [names.FLD_MATCHES_SELECTIONS + "[*]"]  # for deletion
            ]
        )
    ]
)

_OPT_AUTH = KBaseHTTPBearer(optional=True)


def _remove_keys(doc):
    doc = remove_collection_keys(remove_arango_keys(doc))
    doc.pop(names.FLD_MATCHES_SELECTIONS, None)
    doc.pop(names.FLD_SAMPLE_GEO, None)
    return doc


class SamplesTable(TableAttributes, SubsetProcessStates):
    """
    Attributes for a set of samples. Either `fields` and `table` are returned, `data` is
    returned, or `count` is returned.
    The set of available attributes may be different for different collections.
    """


# At some point we're going to want to filter/sort on fields. We may want a list of fields
# somewhere to check input fields are ok... but really we could just fetch the first document
# in the collection and check the fields 
@_ROUTER.get(
    "/",
    response_model=SamplesTable,
    description="Get the corresponding sample attributes for each data unit in the collection, "
        + "which may differ from collection to collection. Note that data units may share "
        + "samples - if so the sample information is duplicated in the table for each data unit."
        + "\n\n "
        + "Authentication is not required unless submitting a match ID or overriding the load "
        + "version; in the latter case service administration permissions are required.\n\n"
        + "When creating selections from samples, use the "
        + f"`{names.FLD_KBASE_ID}` field values as input.")
async def get_samples(
    r: Request,
    collection_id: str = PATH_VALIDATOR_COLLECTION_ID,
    sort_on: common_models.QUERY_VALIDATOR_SORT_ON = names.FLD_KBASE_ID,
    sort_desc: common_models.QUERY_VALIDATOR_SORT_DIRECTION = False,
    skip: common_models.QUERY_VALIDATOR_SKIP = 0,
    limit: common_models.QUERY_VALIDATOR_LIMIT = 1000,
    output_table: common_models.QUERY_VALIDATOR_OUTPUT_TABLE = False,
    count: common_models.QUERY_VALIDATOR_COUNT = False,
    match_id: common_models.QUERY_VALIDATOR_MATCH_ID = None,
    # TODO FEATURE support a choice of AND or OR for matches & selections
    match_mark: common_models.QUERY_VALIDATOR_MATCH_MARK_SAFE = False,
    selection_id: common_models.QUERY_VALIDATOR_SELECTION_ID = None,
    selection_mark: common_models.QUERY_VALIDATOR_SELECTION_MARK_SAFE = False,
    status_only: common_models.QUERY_VALIDATOR_STATUS_ONLY = False,
    load_ver_override: common_models.QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE = None,
    user: kb_auth.KBaseUser = Depends(_OPT_AUTH)
) -> SamplesTable:
    # sorting only works here since we expect the largest collection to be ~300K records and
    # we have a max limit of 1000, which means sorting is O(n log2 1000).
    # Otherwise we need indexes for every sort
    appstate = app_state.get_app_state(r)
    load_ver, dp_match, dp_sel = await get_load_version_and_processes(
        appstate,
        user,
        names.COLL_SAMPLES,
        collection_id,
        ID,
        load_ver_override=load_ver_override,
        match_id=match_id,
        selection_id=selection_id,
    )
    if status_only:
        return _response(dp_match=dp_match, dp_sel=dp_sel)
    if count:
        # for now this method doesn't do much. One we have some filtering implemented
        # it'll need to take that into account.
        # may want to make some sort of shared builder
        count = await count_simple_collection_list(
            appstate.arangostorage,
            names.COLL_SAMPLES,
            collection_id,
            load_ver,
            match_process=dp_match,
            match_mark=match_mark,
            match_prefix=MATCH_ID_PREFIX,
            selection_process=dp_sel,
            selection_mark=selection_mark,
            selection_prefix=SELECTION_ID_PREFIX,
        )
        return _response(dp_match=dp_match, dp_sel=dp_sel, count=count)
    else:
    # TODO SAMPLES data query (like genome attribs)
        return _response(dp_match=dp_match, dp_sel=dp_sel, count=-100)


# TODO SAMPLES location API
# TODO SAMPLES missing IDs API


def _response(
    dp_match: models.DataProductProcess = None,
    dp_sel: models.DataProductProcess = None,
    count: int = None,
    skip: int = 0,
    limit: int = 0,
) -> SamplesTable:
    return SamplesTable(
        skip=skip,
        limit=limit,
        count=count,
        match_state=dp_match.state if dp_match else None,
        selection_state=dp_sel.state if dp_sel else None,
        # TODO SAMPLES data w/ table and dict views
    )
