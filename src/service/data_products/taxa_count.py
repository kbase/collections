"""
The taxa_count data product, which provides taxa counts for a collection at each taxonomy rank.
"""

from fastapi import APIRouter, Request, Depends, Path, Query
from pydantic import BaseModel, Field
from src.common.gtdb_lineage import GTDBTaxaCount
from src.common.product_models.common_models import SubsetProcessStates
from src.common.storage.db_doc_conversions import taxa_node_count_to_doc
import src.common.storage.collection_and_field_names as names
from src.service import app_state
from src.service.app_state_data_structures import PickleableDependencies
from src.service import errors
from src.service import kb_auth
from src.service import models
from src.service import processing_matches
from src.service import processing_selections
from src.service.data_products.common_functions import (
    get_load_version,
    get_collection_singleton_from_db,
    override_load_version,
)
from src.service.data_products.common_models import (
    DataProductSpec,
    DBCollection,
    QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE,
    QUERY_VALIDATOR_STATUS_ONLY,
)
from src.service.data_products import genome_attributes
from src.service.http_bearer import KBaseHTTPBearer
from src.service.routes_common import PATH_VALIDATOR_COLLECTION_ID
from src.service.storage_arango import ArangoStorage, remove_arango_keys
from typing import Any, Annotated


ID = "taxa_count"

_ROUTER = APIRouter(tags=["Taxa count"], prefix=f"/{ID}")

_TYPE2PREFIX = {
    models.SubsetType.MATCH: "m_",
    models.SubsetType.SELECTION: "s_",
}

_MAX_COUNT = 20  # max number of taxa count records to return

# TaxaCount fields. These need to match the field names in the TaxaCount class below
_FLD_TAXA_COUNT_MATCH_COUNT = "match_count"
_FLD_TAXA_COUNT_SEL_COUNT = "sel_count"

# The default sorting order for taxa counts results
_DEFAULT_SORT_ORDER = [names.FLD_TAXA_COUNT_COUNT,
                       _FLD_TAXA_COUNT_MATCH_COUNT,
                       _FLD_TAXA_COUNT_SEL_COUNT]

_INF_NEG = float('-inf')

# The mapping of sort priority to the corresponding field in the taxa count records
_SORT_PRIORITY_ORDER_MAP = {
    "selected": _FLD_TAXA_COUNT_SEL_COUNT,
    "matched": _FLD_TAXA_COUNT_MATCH_COUNT,
    "standard": names.FLD_TAXA_COUNT_COUNT
}


class TaxaCountSpec(DataProductSpec):

    async def delete_match(self, storage: ArangoStorage, internal_match_id: str):
        """
        Delete taxa count match data.

        storage - the storage system
        internal_match_id - the match to delete.
        """
        await delete_match(storage, internal_match_id)

    async def delete_selection(self, storage: ArangoStorage, internal_selection_id: str):
        """
        Delete taxa count selection data.

        storage - the storage system
        internal_selection_id - the selection to delete.
        """
        await delete_selection(storage, internal_selection_id)


TAXA_COUNT_SPEC = TaxaCountSpec(
    data_product=ID,
    router=_ROUTER,
    db_collections=[
        DBCollection(
            name=names.COLL_TAXA_COUNT_RANKS,
            indexes=[]
        ),
        DBCollection(
            name=names.COLL_TAXA_COUNT,
            indexes=[
                [
                    names.FLD_COLLECTION_ID,
                    names.FLD_LOAD_VERSION,
                    names.FLD_INTERNAL_ID,
                    names.FLD_TAXA_COUNT_RANK,
                    names.FLD_TAXA_COUNT_COUNT,
                ],
                [names.FLD_INTERNAL_ID]  # for deleting match / selection data
            ]
        )
    ]
)

_OPT_AUTH = KBaseHTTPBearer(optional=True)


# modifies in place
def _remove_counts_keys(doc: dict):
    for k in [names.FLD_TAXA_COUNT_RANK,
              names.FLD_COLLECTION_ID,
              names.FLD_LOAD_VERSION,
              names.FLD_INTERNAL_ID,
             ]:
        doc.pop(k, None)
    return doc


class Ranks(BaseModel):
    data: list[str] = Field(
        example=["domain", "phylum"],
        description="A list of taxonomy ranks in rank order"
    )


# Note these field names need to match those in src.common.storage.collection_and_field_names
# except for match and selection count
class TaxaCount(BaseModel):
    name: str = Field(
        example="Marininema halotolerans",
        description="The name of the taxa"
    )
    count: int = Field(
        example=42,
        description="The number of genomes in the collection in this taxa"
    )
    match_count: Annotated[int | None, Field(
        example=24,
        description="The number of genomes in the collection in this taxa for the match"
    )] = None
    sel_count: Annotated[int | None, Field(
        example=35,
        description="The number of genomes in the collection in this taxa for the selection"
    )] = None


# these need to match the field names in TaxaCount above
_TYPE2FIELD = {
    models.SubsetType.MATCH: _FLD_TAXA_COUNT_MATCH_COUNT,
    models.SubsetType.SELECTION: _FLD_TAXA_COUNT_SEL_COUNT,
}


class TaxaCounts(SubsetProcessStates):
    """
    The taxa counts data set.
    """
    data: list[TaxaCount] | None = None


_FLD_COL_ID = "colid"
_FLD_KEY = "key"
_FLD_COL_NAME = "colname"
_FLD_COL_LV = "colload"
_FLD_COL_RANK = "rank"
_FLD_COL_NAME_LIST = "colnamelist"


@_ROUTER.get(
    "/ranks/",
    response_model=Ranks,
    description="Get the taxonomy ranks, in rank order, for the taxa counts.\n\n "
        + "Authentication is not required unless overriding the load version, in which case "
        + "service administration permissions are required.")
async def get_ranks(
    r: Request,
    collection_id: str = PATH_VALIDATOR_COLLECTION_ID,
    load_ver_override: QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE = None,
    user: kb_auth.KBaseUser = Depends(_OPT_AUTH)
):
    store = app_state.get_app_state(r).arangostorage
    _, load_ver = await get_load_version(store, collection_id, ID, load_ver_override, user)
    return await get_ranks_from_db(store, collection_id, load_ver, bool(load_ver_override))


async def get_ranks_from_db(
    store: ArangoStorage,
    collection_id: str,
    load_ver: str,
    load_ver_overridden: bool,
):
    doc = await get_collection_singleton_from_db(
        store, names.COLL_TAXA_COUNT_RANKS, collection_id, load_ver, load_ver_overridden)
    return Ranks(data=doc[names.FLD_TAXA_COUNT_RANKS])


@_ROUTER.get(
    "/counts/{rank}/",
    response_model=TaxaCounts,
    description="Get the taxonomy counts in descending order. At most 20 taxa are returned.\n\n "
        + "Authentication is not required unless providing a match ID or overriding the load "
        + "version; in the latter case service administration permissions are required.")
async def get_taxa_counts(
    r: Request,
    collection_id: str = PATH_VALIDATOR_COLLECTION_ID,
    # it'd be nice if this could be a enum, but different counts might have different ranks
    rank: str = Path(
        example="phylum",
        description="The taxonomic rank at which to return results"
    ),
    match_id: str = Query(
        default = None,
        description="A match ID to include the match count in the taxa count data. "
            + "Authentication is required. "
            # matches are against a specific load version, so...
            + "Note that if a match ID is set, any load version override is ignored."),
    selection_id: str = Query(
        default = None,
        description="A selection ID to include the selection count in the taxa count data. "
            + "Note that if a selection ID is set, any load version override is ignored."),
    status_only: QUERY_VALIDATOR_STATUS_ONLY = False,
    load_ver_override: QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE = None,
    user: kb_auth.KBaseUser = Depends(_OPT_AUTH),
    sort_priority: list[str] = Query(
        default=list(_SORT_PRIORITY_ORDER_MAP.keys()),
        description="Specify the priority order for sorting taxa counts based on the provided fields and "
                    "their corresponding order.",
    )
):
    appstate = app_state.get_app_state(r)
    store = appstate.arangostorage
    dp_match, dp_sel = None, None
    lvo = override_load_version(load_ver_override, match_id, selection_id)
    coll, load_ver = await get_load_version(appstate.arangostorage, collection_id, ID, lvo, user)
    _check_genome_attribs(coll, bool(match_id), bool(selection_id))
    if match_id:
        dp_match = await processing_matches.get_or_create_data_product_match_process(
            appstate, coll, user, match_id, ID, _process_taxa_count_subset
        )
    if selection_id:
        dp_sel = await processing_selections.get_or_create_data_product_selection_process(
            appstate, coll, selection_id, ID, _process_taxa_count_subset
        )
    if status_only:
        return _taxa_counts(dp_match=dp_match, dp_sel=dp_sel)
    ranks = await get_ranks_from_db(store, collection_id, load_ver, bool(load_ver_override))
    if rank not in ranks.data:
        raise errors.IllegalParameterError(f"Invalid rank: {rank}")
    q = await _query(store, collection_id, load_ver, rank)
    for dp_proc in [dp_match, dp_sel]:
        await _add_subset_data_in_place(q, store, collection_id, load_ver, rank, dp_proc)

    _sort_taxa_counts(q, sort_priority, [dp_match, dp_sel])

    return _taxa_counts(dp_match=dp_match, dp_sel=dp_sel, data=q)


def _fill_missing_orders(sort_order: list[str]):
    # fill in missing orders with the default sort order
    if not isinstance(sort_order, list):
        raise ValueError(f"sort_order must be a list of strings, provided: {sort_order}")

    return [order for order in _DEFAULT_SORT_ORDER if order not in sort_order] + sort_order


def _sort_taxa_counts(
        q: list[dict[str, Any]],
        sort_priority: list[str],
        dp_list: list[models.DataProductProcess]):
    # Sort taxa count records in place by the sort_priority list.
    processed_count = [_TYPE2FIELD[dp.type] for dp in dp_list if dp]

    if processed_count:

        if len(sort_priority) != len(set(sort_priority)):
            raise errors.IllegalParameterError(f"Duplicate sort priority found: {sort_priority}")

        try:
            sort_order_rev = [_SORT_PRIORITY_ORDER_MAP[k] for k in sort_priority]
        except KeyError as e:
            raise errors.IllegalParameterError(f"Invalid sort priority: {e}, "
                                               f"valid priorities are: {list(_SORT_PRIORITY_ORDER_MAP.keys())}")

        # fill in missing orders with the default precedence order
        sort_order = _fill_missing_orders(sort_order_rev[::-1])
        # remove any sort orders that are not processed (i.e. match or selection counts)
        sort_order = [order for order in sort_order if order in processed_count or order == names.FLD_TAXA_COUNT_COUNT]
    else:
        sort_order = [names.FLD_TAXA_COUNT_COUNT]

    for k in sort_order:
        q.sort(key=lambda x: x[k], reverse=True)


def _taxa_counts(
    dp_match: models.DataProductProcess = None,
    dp_sel: models.DataProductProcess = None,
    data: dict[str, Any] = None,
) -> TaxaCounts:
    return TaxaCounts(
        match_state=dp_match.state if dp_match else None,
        selection_state=dp_sel.state if dp_sel else None,
        data=data,
    )


async def _add_subset_data_in_place(
    q: dict[str, Any],
    store: ArangoStorage,
    collection_id: str,
    load_ver: str,
    rank: str,
    dp_process: models.DataProductProcess,
):
    if dp_process and dp_process.is_complete():
        matchq = await _query(
            store,
            collection_id,
            load_ver,
            rank,
            internal_id=dp_process.internal_id,
            type_=dp_process.type,
        )
        name, count = names.FLD_TAXA_COUNT_NAME, names.FLD_TAXA_COUNT_COUNT
        mqd = {d[name]: d[count] for d in matchq}
        for d in q:
            d[_TYPE2FIELD[dp_process.type]] = mqd.get(d[name], 0)


def _check_genome_attribs(coll: models.ActiveCollection, match: bool, selection: bool):
    # I'm kind of uncomfortable hard coding this dependency... but it's real so... I dunno.
    # Might need refactoring later once it become more clear how data products should
    # interact.
    # Maybe just don't allow registering a taxa count DP without a genome attribs DP, and
    # configure that in the taxa count spec
    if (coll and (match or selection) and
        genome_attributes.ID not in {dp.product for dp in coll.data_products}):
            errclass = (errors.InvalidMatchStateError if match
                        else errors.InvalidSelectionStateError)
            raise errclass(
                f"Cannot perform a {ID} subset when the collection does not have a "
                + f"{genome_attributes.ID} data product")


async def _query(
    store: ArangoStorage,
    collection_id: str,
    load_ver: str,
    rank: str,
    internal_id: str | None = None,
    type_: models.SubsetType | None = None,
    name_list: list[str] = None,
    limit: int = _MAX_COUNT
):
    """
    Query the taxa count data.

    store - the storage system
    collection_id - the ID of the Collection for which to retrieve the taxa counts information
    load_ver - the load version of the collection.
    rank - the rank at which to retrieve the taxa counts
    internal_id - the internal ID of a related match or selection, if any
    type_ - the type of the subset data, match or selection
    name_list - a list of names for filtering by matching the 'name' field
    limit - the maximum number of records to return.
    """
    if internal_id:
        internal_id = _TYPE2PREFIX[type_] + internal_id
    bind_vars = {
        f"@{_FLD_COL_NAME}": names.COLL_TAXA_COUNT,
        _FLD_COL_ID: collection_id,
        _FLD_COL_LV: load_ver,
        _FLD_COL_RANK: rank,
        "internal_id": internal_id,
    }

    aql_name_list = ""
    if name_list:
        bind_vars[_FLD_COL_NAME_LIST] = name_list
        aql_name_list = f"FILTER d.{names.FLD_TAXA_COUNT_NAME} IN @{_FLD_COL_NAME_LIST}"

    # will probably want some sort of sort / limit options, but don't get too crazy. Wait for
    # feedback for now
    aql = f"""
        FOR d IN @@{_FLD_COL_NAME}
            FILTER d.{names.FLD_COLLECTION_ID} == @{_FLD_COL_ID}
            FILTER d.{names.FLD_LOAD_VERSION} == @{_FLD_COL_LV}
            FILTER d.{names.FLD_INTERNAL_ID} == @internal_id
            FILTER d.{names.FLD_TAXA_COUNT_RANK} == @{_FLD_COL_RANK}
            {aql_name_list}
            SORT d.{names.FLD_TAXA_COUNT_COUNT} DESC
            LIMIT {limit}
            RETURN d
    """

    # could get the doc count, but that'll be slower since all docs have to be counted vs. just
    # getting LIMIT docs. YAGNI for now
    cur = await store.execute_aql(aql, bind_vars=bind_vars)
    ret = []
    try:
        async for d in cur:
            # not clear there's any benefit to creating a bunch of TaxaCount objects here
            ret.append(_remove_counts_keys(remove_arango_keys(d)))
    finally:
        await cur.close(ignore_missing=True)
    return ret


async def _process_taxa_count_subset(
    deps: PickleableDependencies,
    storage: ArangoStorage,
    match_or_sel: models.InternalMatch | models.InternalSelection,
    coll: models.SavedCollection,
    dpid: models.DataProductProcessIdentifier,
):
    load_ver = {dp.product: dp.version for dp in coll.data_products}[ID]
    # Right now this is hard coded to use the GTDB lineage, which is the only choice we
    # have. Might need to expand in future for other count strategies.
    # Maybe a collection parameter, which would be available from the collection data structure
    # given the matcher ID.
    count = GTDBTaxaCount()
    await genome_attributes.process_subset_documents(
        storage,
        coll,
        dpid.internal_id,
        dpid.type,
        lambda doc: count.add(doc[names.FLD_GENOME_ATTRIBS_GTDB_LINEAGE]),
        fields=[names.FLD_GENOME_ATTRIBS_GTDB_LINEAGE]
    )
    docs = [
        taxa_node_count_to_doc(
            coll.id, load_ver, tc, _TYPE2PREFIX[dpid.type] + dpid.internal_id
        ) for tc in count
    ]
    # May need to batch this?
    # The other option I considered here was adding the match counts to the records directly.
    # That is going to be way slower, since you have to update one record at a time,
    # and adds a lot of complication since you'll need to store many matches in the same
    # record. For now go with separate records for matches for speed and simplicity. If
    # we need to refactor later we'll do it when the user story exists.
    # Ignore collisions so that if two match processes get kicked off at once the result
    # is the same and neither fails.
    await storage.import_bulk_ignore_collisions(names.COLL_TAXA_COUNT, docs)
    await storage.update_data_product_process_state(
        dpid, models.ProcessState.COMPLETE, deps.get_epoch_ms())


async def delete_match(storage: ArangoStorage, internal_match_id: str):
    """
    Delete taxa count match data.

    storage - the storage system
    internal_match_id - the match to delete.
    """
    await _delete_subset(storage, internal_match_id, models.SubsetType.MATCH)


async def delete_selection(storage: ArangoStorage, internal_selection_id: str):
    """
    Delete taxa count selection data.

    storage - the storage system
    internal_selection_id - the selection to delete.
    """
    await _delete_subset(storage, internal_selection_id, models.SubsetType.SELECTION)


async def _delete_subset(storage: ArangoStorage, internal_id: str, type_: models.SubsetType):
    bind_vars = {
        f"@{_FLD_COL_NAME}": names.COLL_TAXA_COUNT,
        "internal_id": _TYPE2PREFIX[type_] + internal_id,
    }
    aql = f"""
        FOR d IN @@{_FLD_COL_NAME}
            FILTER d.{names.FLD_INTERNAL_ID} == @internal_id
            REMOVE d IN @@{_FLD_COL_NAME}
            OPTIONS {{exclusive: true}}
    """
    cur = await storage.execute_aql(aql, bind_vars=bind_vars)
    await cur.close(ignore_missing=True)
