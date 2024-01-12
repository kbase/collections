"""
The samples data product, which provides sample information for a collection.
"""
from typing import Any, Annotated

from fastapi import APIRouter, Request, Depends, Query
from pydantic import BaseModel, Field

import src.common.storage.collection_and_field_names as names
from src.common.product_models.common_models import SubsetProcessStates
from src.service import app_state, errors, kb_auth, models
from src.service.data_products.common_functions import (
    remove_collection_keys,
    remove_marked_subset,
    query_table,
    get_load_version,
    QueryTableResult
)
from src.service.data_products import common_models
from src.service.data_products.data_product_processing import (
    MATCH_ID_PREFIX,
    SELECTION_ID_PREFIX,
    get_load_version_and_processes,
    get_missing_ids as _get_missing_ids
)
from src.service.data_products.table_models import TableAttributes
from src.service.filtering.filters import FilterSet
from src.service.http_bearer import KBaseHTTPBearer
from src.service.processing import SubsetSpecification
from src.service.routes_common import PATH_VALIDATOR_COLLECTION_ID
from src.service.storage_arango import ArangoStorage, remove_arango_keys

# Implementation note - we know FLD_KBASE_ID is unique per collection id /
# load version combination since the loader uses those 3 fields as the arango _key

# Implementation note 2 - we were directed to make a flat record, e.g. 1 sample record for each
# kbase_id, even though there are many kbase_ids per sample. This will probably need to be
# reworked later to remove the many duplicate sample records and instead have some kind of
# M:1 kbase_id:sample relationship.

ID = "samples"

_ROUTER = APIRouter(tags=["Samples"], prefix=f"/{ID}")

_MAX_SAMPLE_IDS = 1000


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
                    names.FLD_KB_SAMPLE_ID,
                    # Since this is the default sort option (see below), we specify an index
                    # for fast sorts since every time the user hits the UI for the first time
                    # or without specifying a sort order it'll sort on this field
                ],
                [
                    names.FLD_COLLECTION_ID,
                    names.FLD_LOAD_VERSION,
                    names.FLD_KBASE_IDS,
                    # Find kbase IDs for matching / selection marking
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
                    names.FLD_KB_SAMPLE_ID,
                    # for finding matches/selections, and opt a default sort on the kbase sample ID
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
    doc.pop(names.FLD_KBASE_IDS, None)
    return doc


class SamplesTable(TableAttributes, SubsetProcessStates):
    """
    Attributes for a set of samples. Either `fields` and `table` are returned, `data` is
    returned, or `count` is returned.
    The set of available attributes may be different for different collections.
    """

class SampleLocation(BaseModel):
    """
    A location of one or more samples containing one or more genomes, and optionally
    the IDs of the samples.
    """
    lat: float = Field(example=36.1, description="The latitude of the location in degrees.")
    lon: float = Field(example=-28.2, description="The longitude of the location in degrees.")
    count: int = Field(example=3, description="The number of genomes found at the location.")
    ids: Annotated[list[str] | None, Field(
        example=["993eeea2-5323-44dd-80d5-18b1f7cb57bf"],
        description="The sample IDs found at the location."
    )] = None


class SampleLocations(SubsetProcessStates):
    """
    A list of sample locations, aggregated by location.
    """
    locs: list[SampleLocation] | None = None


class Samples(BaseModel):
    """
    A list of samples with their attributes.
    """
    samples: list[dict[str, Any]] = Field(
        example=[{names.FLD_KB_SAMPLE_ID: "993eeea2-5323-44dd-80d5-18b1f7cb57bf"}],
        description="The sample attributes as a list of dictionaries, one sample per list entry."
    )


# At some point we're going to want to filter/sort on fields. We may want a list of fields
# somewhere to check input fields are ok... but really we could just fetch the first document
# in the collection and check the fields 
@_ROUTER.get(
    "/",
    response_model=SamplesTable,
    description="Get the sample attributes for the data units in the collection, "
        + "which may differ from collection to collection.\n\n"
        + "Authentication is not required unless submitting a match ID or overriding the load "
        + "version; in the latter case service administration permissions are required.\n\n"
    # TODO SAMPLES - how should we support creating a selection from samples?
)
async def get_samples(
    r: Request,
    collection_id: str = PATH_VALIDATOR_COLLECTION_ID,
    sort_on: Annotated[str, Query(
        example=names.FLD_KB_SAMPLE_ID,
        description="The field to sort on."
    )] = names.FLD_KB_SAMPLE_ID,
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
        multiple_ids=True,
    )
    if status_only:
        return _response(dp_match=dp_match, dp_sel=dp_sel)
    filters = FilterSet(
        collection_id,
        load_ver,
        collection=names.COLL_SAMPLES,
        count=count,
        match_spec=SubsetSpecification(
            subset_process=dp_match, mark_only=match_mark, prefix=MATCH_ID_PREFIX),
        selection_spec=SubsetSpecification(
            subset_process=dp_sel, mark_only=selection_mark, prefix=SELECTION_ID_PREFIX),
        sort_on=sort_on,
        sort_descending=sort_desc,
        skip=skip,
        limit=limit,
    )
    res = await query_table(appstate.arangostorage, filters, output_table, _remove_keys)
    return _response(dp_match=dp_match, dp_sel=dp_sel, res=res)


def _response(
    dp_match: models.DataProductProcess = None,
    dp_sel: models.DataProductProcess = None,
    res: QueryTableResult = None,
) -> SamplesTable:
    if res:
        return SamplesTable(
            skip=res.skip,
            limit=res.limit,
            count=res.count,
            match_state=dp_match.state if dp_match else None,
            selection_state=dp_sel.state if dp_sel else None,
            fields=res.fields,
            table=res.table,
            data=res.data,
        )
    else:
        return SamplesTable(
            skip=0,
            limit=0,
            match_state=dp_match.state if dp_match else None,
            selection_state=dp_sel.state if dp_sel else None,
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
    include_sample_ids: Annotated[bool, Query(
        description="Whether to include the sample IDs present at each location.\n\n"
            + "**WARNING**: If there are many samples in the collection, this call may take a long "
            + "time and a lot of memory. In the future it may be disallowed for collections with "
            + "large numbers of samples."
    )] = False,
    match_id: Annotated[str, Query(
        description="A match ID to set the view to the match rather than "
            + "the entire collection. Authentication is required. If a match ID is "
            # matches are against a specific load version, so...
            + "set, any load version override is ignored. "
            + "If a selection filter and a match filter are provided, they are ANDed together. "
    )] = None,
    # TODO FEATURE support a choice of AND or OR for matches & selections
    selection_id: Annotated[str, Query(
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
        multiple_ids=True,
    )
    if status_only:
        return _location_response(dp_match=dp_match, dp_sel=dp_sel)
    return await _query_location(
        appstate.arangostorage, collection_id, load_ver, dp_match, dp_sel, include_sample_ids)


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


async def _query_location(
    storage: ArangoStorage,
    collection_id: str,
    load_ver: str,
    dp_match: models.DataProductProcess = None,
    dp_sel: models.DataProductProcess = None,
    include_sample_ids = False,
) -> SampleLocation:
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
    if include_sample_ids:
        aql += f"""
            COLLECT lat = d.{names.FLD_SAMPLE_LATITUDE}, lon = d.{names.FLD_SAMPLE_LONGITUDE}
                AGGREGATE sampleids = UNIQUE(d.{names.FLD_KB_SAMPLE_ID}),
                          count = SUM(d.{names.FLD_KB_GENOME_COUNT})
            RETURN {{
                "lat": lat,
                "lon": lon,
                "count": count,
                "sampleids": sampleids
            }}
        """
    else:
        aql += f"""
            COLLECT lat = d.{names.FLD_SAMPLE_LATITUDE}, lon = d.{names.FLD_SAMPLE_LONGITUDE}
                AGGREGATE count = SUM(d.{names.FLD_KB_GENOME_COUNT})
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
            if include_sample_ids:
                res.append(SampleLocation(
                    lat=d["lat"], lon=d["lon"], count=d["count"], ids=d["sampleids"]
                ))
            else:
                res.append(SampleLocation(lat=d["lat"], lon=d["lon"], count=d["count"]))
    finally:
        await cur.close(ignore_missing=True)
    return _location_response(dp_match=dp_match, dp_sel=dp_sel, locs=res)


@_ROUTER.get(
    "/byid",
    response_model=Samples,
    summary=f"Get samples by ID.",
    description=f"Provide specific sample IDs and get the sample attributes for those IDs.\n\n"
        + "Authentication is not required unless overriding the load version, in which case "
        + "service administration permissions are required.\n\n",
)
async def get_samples_by_id(
    r: Request,
    collection_id: Annotated[str, PATH_VALIDATOR_COLLECTION_ID],
    sample_ids: Annotated[str, Query(
        example="993eeea2-5323-44dd-80d5-18b1f7cb57bf, 2e72b2de-d72f-4e0a-9192-2b961d61aa22",
        description=f"A list of sample IDs, separated by commas. At most {_MAX_SAMPLE_IDS} "
            + "sample IDs are allowed."
    )],
    load_ver_override: common_models.QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE = None,
    user: kb_auth.KBaseUser = Depends(_OPT_AUTH),
):
    storage = app_state.get_app_state(r).arangostorage
    _, load_ver = await get_load_version(
        storage, collection_id, ID, load_ver_override, user)
    if not sample_ids or not sample_ids.strip():
        return Samples(samples=[])
    sample_ids = {s.strip() for s in sample_ids.split(",")}
    if len(sample_ids) > _MAX_SAMPLE_IDS:
        raise errors.IllegalParameterError(
            f"No more than {_MAX_SAMPLE_IDS} sample IDs are allowed")
    bind_vars = {
        f"@coll": names.COLL_SAMPLES,
        "coll_id": collection_id,
        "load_ver": load_ver,
        "sampleids": list(sample_ids)
    }
    aql = f"""
        FOR d IN @@coll
            FILTER d.{names.FLD_COLLECTION_ID} == @coll_id
            FILTER d.{names.FLD_LOAD_VERSION} == @load_ver
            FILTER d.{names.FLD_KB_SAMPLE_ID} IN @sampleids
            RETURN d
    """
    res = []
    found_ids = set()
    cur = await storage.execute_aql(aql, bind_vars=bind_vars)
    try:
        async for d in cur:
            d = _remove_keys(d)
            res.append(d)
            found_ids.add(d[names.FLD_KB_SAMPLE_ID])
    finally:
        await cur.close(ignore_missing=True)
    missing_ids = sample_ids - found_ids
    if missing_ids:
        err = f"Some provided sample IDs were not found in {collection_id} load version {load_ver}"
        if len(missing_ids) <= 10:
            err += f": {sorted(missing_ids)}"
        else:
            err += f"; showing 10: {list(sorted(missing_ids))[:10]}"
        raise errors.NoDataFoundError(err)
    return Samples(samples=res)


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
    match_id: Annotated[str, Query(description="A match ID.")] = None,
    selection_id: Annotated[str, Query(description="A selection ID.")] = None,
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
        multiple_ids=True,
    )
