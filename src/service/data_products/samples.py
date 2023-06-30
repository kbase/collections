"""
The samples data product, which provides sample information for a collection.
"""

from collections import defaultdict
import logging

from fastapi import APIRouter, Request, Depends, Query
from pydantic import BaseModel, Field
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
    count_simple_collection_list,
    mark_data_by_kbase_id,
    remove_marked_subset,
    override_load_version,
    query_table
)
from src.service.data_products import common_models
from src.service.data_products.data_product_processing import (
    MATCH_ID_PREFIX,
    SELECTION_ID_PREFIX,
    get_load_version_and_processes,
    get_missing_ids as _get_missing_ids
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

class SampleLocation(BaseModel):
    """
    A location of one or more samples containing one or more genomes.
    """
    lat: float = Field(example=36.1, description="The latitude of the location in degrees.")
    lon: float = Field(example=-28.2, description="The longitude of the location in degrees.")
    count: int = Field(example=3, description="The number of genomes found at the location.")


class SampleLocations(SubsetProcessStates):
    """
    A list of sample locations, aggregated by location.
    """
    locs: list[SampleLocation] | None = None



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
    output_table: common_models.QUERY_VALIDATOR_OUTPUT_TABLE = True,
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
        res = await query_table(
            appstate.arangostorage,
            names.COLL_SAMPLES,
            collection_id,
            load_ver,
            sort_on,
            sort_descending=sort_desc,
            skip=skip,
            limit=limit,
            output_table=output_table,
            match_process=dp_match,
            match_mark=match_mark,
            match_prefix=MATCH_ID_PREFIX,
            selection_process=dp_sel,
            selection_mark=selection_mark,
            selection_prefix=SELECTION_ID_PREFIX,
            document_mutator=_remove_keys,
        )
        return _response(
            skip=res.skip,
            limit=res.limit,
            dp_match=dp_match,
            dp_sel=dp_sel,
            fields=res.fields,
            table=res.table,
            data=res.data
        )


def _response(
    dp_match: models.DataProductProcess = None,
    dp_sel: models.DataProductProcess = None,
    count: int = None,
    skip: int = 0,
    limit: int = 0,
    fields: list[dict[str, str]] = None,
    table: list[list[Any]] = None,
    data: list[dict[str, Any]] = None,
) -> SamplesTable:
    return SamplesTable(
        skip=skip,
        limit=limit,
        count=count,
        match_state=dp_match.state if dp_match else None,
        selection_state=dp_sel.state if dp_sel else None,
        fields=fields,
        table=table,
        data=data,
    )


def _get_subset_id(subset_process: models.DataProductProcess, subset_prefix: str):
    if subset_process and subset_process.is_complete():
        return subset_prefix + subset_process.internal_id
    return None


@_ROUTER.get(
    "/locations",
    response_model=SampleLocations,
    description="Get sample locations and the number of genomes at each location. "
        + "Currently all sample locations are returned."
        + "\n\n "
        + "Authentication is not required unless submitting a match ID or overriding the load "
        + "version; in the latter case service administration permissions are required.\n"
)
async def get_sample_locations(
    r: Request,
    collection_id: str = PATH_VALIDATOR_COLLECTION_ID,
    match_id: Annotated[str | None, Query(
        description="A match ID to set the view to the match rather than "
            + "the entire collection. Authentication is required. If a match ID is "
            # matches are against a specific load version, so...
            + "set, any load version override is ignored. "
            + "If a selection filter and a match filter are provided, they are ANDed together. "
    )] = None,
    # TODO FEATURE support a choice of AND or OR for matches & selections
    selection_id: Annotated[str | None, Query(
        description="A selection ID to set the view to the selection rather than the entire "
            + "collection. If a selection ID is set, any load version override is ignored. "
            + "If a selection filter and a match filter are provided, they are ANDed together. "
    )] = None,
    status_only: common_models.QUERY_VALIDATOR_STATUS_ONLY = False,
    load_ver_override: common_models.QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE = None,
    user: kb_auth.KBaseUser = Depends(_OPT_AUTH)
) -> SampleLocations:
    # might need to return a bare Response if the pydantic checking gets too expensive
    # might need some sort of pagination
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
        return _location_response(dp_match=dp_match, dp_sel=dp_sel)
    return await _query(appstate.arangostorage, collection_id, load_ver, dp_match, dp_sel)


def _location_response(
    dp_match: models.DataProductProcess = None,
    dp_sel: models.DataProductProcess = None,
    locs: list[SampleLocation] = None
) -> SampleLocations:
    return SampleLocations(
        match_state=dp_match.state if dp_match else None,
        selection_state=dp_sel.state if dp_sel else None,
        locs=locs
    )


async def _query(
    storage: ArangoStorage,
    collection_id: str,
    load_ver: str,
    dp_match: models.DataProductProcess = None,
    dp_sel: models.DataProductProcess = None,
):
    internal_match_id = _get_subset_id(dp_match, MATCH_ID_PREFIX)
    internal_selection_id = _get_subset_id(dp_sel, SELECTION_ID_PREFIX)
    bind_vars = {
        f"@coll": names.COLL_SAMPLES,
        "coll_id": collection_id,
        "load_ver": load_ver,
    }
    aql = f"""
    FOR d IN @@coll
        FILTER d.{names.FLD_COLLECTION_ID} == @coll_id
        FILTER d.{names.FLD_LOAD_VERSION} == @load_ver
    """
    if internal_match_id:
        bind_vars["internal_match_id"] = internal_match_id
        aql += f"""
            FILTER @internal_match_id IN d.{names.FLD_MATCHES_SELECTIONS}
        """
    # this will AND the match and selection. To OR, just OR the two filters instead of having
    # separate statements.
    if internal_selection_id:
        bind_vars["internal_selection_id"] = internal_selection_id
        aql += f"""
            FILTER @internal_selection_id IN d.{names.FLD_MATCHES_SELECTIONS}
        """
    aql += f"""
        COLLECT lat = d.{names.FLD_SAMPLE_LATITUDE}, lon = d.{names.FLD_SAMPLE_LONGITUDE}
            WITH COUNT INTO count
        RETURN {{
            "lat": lat,
            "lon": lon,
            "count": count
        }}
    """
    res = []
    cur = await storage.execute_aql(aql, bind_vars=bind_vars)
    try:
        async for d in cur:
            res.append(SampleLocation(lat=d["lat"], lon=d["lon"], count=d["count"]))
    finally:
        await cur.close(ignore_missing=True)
    return _location_response(dp_match=dp_match, dp_sel=dp_sel, locs=res)


@_ROUTER.get(
    "/missing",
    response_model=common_models.DataProductMissingIDs,
    summary=f"Get missing IDs for a match or selection",
    description=f"Get the list of genome IDs that were not found in these samples "
        + "but were present in the match and / or selection.",
)
async def get_missing_ids(
    r: Request,
    collection_id: Annotated[str, PATH_VALIDATOR_COLLECTION_ID],
    match_id: Annotated[str | None, Query(description="A match ID.")] = None,
    selection_id: Annotated[str | None, Query(description="A selection ID.")] = None,
    user: kb_auth.KBaseUser = Depends(_OPT_AUTH),
) -> common_models.DataProductMissingIDs:
    return await _get_missing_ids(
        app_state.get_app_state(r),
        names.COLL_SAMPLES,
        collection_id,
        ID,
        match_id=match_id,
        selection_id=selection_id,
        user=user,
    )

