"""
The taxa_count data product, which provides taxa counts for a collection at each taxonomy rank.
"""

import logging
from fastapi import APIRouter, Request, Depends, Path, Query
from pydantic import BaseModel, Field
from src.common.hash import md5_string
from src.common.gtdb_lineage import GTDBTaxaCount
from src.common.storage.db_doc_conversions import taxa_node_count_to_doc
import src.common.storage.collection_and_field_names as names
from src.service import app_state
from src.service.app_state_data_structures import PickleableDependencies, CollectionsState
from src.service import errors
from src.service import kb_auth
from src.service import models
from src.service import processing
from src.service import processing_matches
from src.service import processing_selections
from src.service.data_products.common_functions import (
    get_load_version,
    get_load_ver_from_collection,
    get_collection_singleton_from_db,
)
from src.service.data_products.common_models import (
    DataProductSpec,
    DBCollection,
    QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE,
)
from src.service.data_products import genome_attributes
from src.service.http_bearer import KBaseHTTPBearer
from src.service.routes_common import PATH_VALIDATOR_COLLECTION_ID
from src.service.storage_arango import ArangoStorage, remove_arango_keys
from typing import Any


ID = "taxa_count"

_ROUTER = APIRouter(tags=["Taxa count"], prefix=f"/{ID}")

_TYPE2PREFIX = {
    models.SubsetType.MATCH: "m_",
    models.SubsetType.SELECTION: "s_",
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
    match_count: int | None = Field(
        example=24,
        description="The number of genomes in the collection in this taxa for the match"
    )
    sel_count: int | None = Field(
        example=35,
        description="The number of genomes in the collection in this taxa for the selection"
    )


# these need to match the field names in TaxaCount above
_TYPE2FIELD = {
    models.SubsetType.MATCH: "match_count",
    models.SubsetType.SELECTION: "sel_count",
}


class TaxaCounts(BaseModel):
    """
    The taxa counts data set.
    """
    taxa_count_match_state: models.ProcessState | None = Field(
        example=models.ProcessState.PROCESSING,
        description="The processing state of the match (if any) for this data product. "
            + "This data product requires additional processing beyond the primary match."
    )
    taxa_count_selection_state: models.ProcessState | None = Field(
        example=models.ProcessState.FAILED,
        description="The processing state of the selection (if any) for this data product. "
            + "This data product requires additional processing beyond the primary selection."
    )
    data: list[TaxaCount] | None


_FLD_COL_ID = "colid"
_FLD_KEY = "key"
_FLD_COL_NAME = "colname"
_FLD_COL_LV = "colload"
_FLD_COL_RANK = "rank"


@_ROUTER.get(
    "/ranks/",
    response_model=Ranks,
    description="Get the taxonomy ranks, in rank order, for the taxa counts.\n\n "
        + "Authentication is not required unless overriding the load version, in which case "
        + "service administration permissions are required.")
async def get_ranks(
    r: Request,
    collection_id: str = PATH_VALIDATOR_COLLECTION_ID,
    load_ver_override: str | None = QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE,
    user: kb_auth.KBaseUser = Depends(_OPT_AUTH)
):
    store = app_state.get_app_state(r).arangostorage
    load_ver = await get_load_version(store, collection_id, ID, load_ver_override, user)
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
    match_id: str | None = Query(
        default = None,
        description="A match ID to include the match count in the taxa count data. "
            + "Authentication is required. "
            # matches are against a specific load version, so...
            + "Note that if a match ID is set, any load version override is ignored."),
    selection_id: str | None = Query(
        default = None,
        description="A selection ID to include the selection count in the taxa count data. "
            + "Note that if a selection ID is set, any load version override is ignored."),
    load_ver_override: str | None = QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE,
    user: kb_auth.KBaseUser = Depends(_OPT_AUTH)
):
    appstate = app_state.get_app_state(r)
    store = appstate.arangostorage
    dp_match, dp_sel = None, None
    if match_id or selection_id:
        errclass = errors.InvalidMatchStateError if match_id else errors.InvalidSelectionStateError
        load_ver, coll = await _get_load_ver(appstate, collection_id, errclass)
    else:
        load_ver = await get_load_version(store, collection_id, ID, load_ver_override, user)
    if match_id:
        dp_match = await processing_matches.get_or_create_data_product_match_process(
            appstate, coll, user, match_id, ID, _process_taxa_count_subset
        )
    if selection_id:
        dp_sel = await processing_selections.get_or_create_data_product_selection_process(
            appstate, coll, selection_id, ID, _process_taxa_count_subset
        )
    ranks = await get_ranks_from_db(store, collection_id, load_ver, bool(load_ver_override))
    if rank not in ranks.data:
        raise errors.IllegalParameterError(f"Invalid rank: {rank}")
    q = await _query(store, collection_id, load_ver, rank)
    for dp_proc in [dp_match, dp_sel]:
        await _add_subset_data_in_place(q, store, collection_id, load_ver, rank, dp_proc)
        # For now always sort by the std data. See if ppl want sort by match/selection data
        # before implementing something more sophisticated.
    return TaxaCounts(
        taxa_count_match_state=dp_match.state if dp_match else None,
        taxa_count_selection_state=dp_sel.state if dp_sel else None,
        data=q,
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
            dp_process.internal_id,
            dp_process.type,
        )
        name, count = names.FLD_TAXA_COUNT_NAME, names.FLD_TAXA_COUNT_COUNT
        mqd = {d[name]: d[count] for d in matchq}
        for d in q:
            d[_TYPE2FIELD[dp_process.type]] = mqd.get(d[name], 0)


async def _get_load_ver(appstate: CollectionsState, collection_id: str, errclass
) -> tuple[str, models.SavedCollection]:
    coll = await appstate.arangostorage.get_collection_active(collection_id)
    # I'm kind of uncomfortable hard coding this dependency... but it's real so... I dunno.
    # Might need refactoring later once it become more clear how data products should
    # interact.
    if genome_attributes.ID not in {dp.product for dp in coll.data_products}:
        raise errclass(
            f"Cannot perform a {ID} subset when the collection does not have a "
            + f"{genome_attributes.ID} data product")
    load_ver = get_load_ver_from_collection(coll, ID)
    return load_ver, coll


async def _query(
    store: ArangoStorage,
    collection_id: str,
    load_ver: str,
    rank: str,
    internal_id: str | None = None,
    type_: models.SubsetType | None = None
):
    if internal_id:
        internal_id = _TYPE2PREFIX[type_] + internal_id
    bind_vars = {
        f"@{_FLD_COL_NAME}": names.COLL_TAXA_COUNT,
        _FLD_COL_ID: collection_id,
        _FLD_COL_LV: load_ver,
        _FLD_COL_RANK: rank,
        "internal_id": internal_id,
    }
    # will probably want some sort of sort / limit options, but don't get too crazy. Wait for
    # feedback for now
    aql = f"""
        FOR d IN @@{_FLD_COL_NAME}
            FILTER d.{names.FLD_COLLECTION_ID} == @{_FLD_COL_ID}
            FILTER d.{names.FLD_LOAD_VERSION} == @{_FLD_COL_LV}
            FILTER d.{names.FLD_INTERNAL_ID} == @internal_id
            FILTER d.{names.FLD_TAXA_COUNT_RANK} == @{_FLD_COL_RANK}
            SORT d.{names.FLD_TAXA_COUNT_COUNT} DESC
            LIMIT 20
            RETURN d
    """
    # could get the doc count, but that'll be slower since all docs have to be counted vs. just
    # getting LIMIT docs. YAGNI for now
    cur = await store.aql().execute(aql, bind_vars=bind_vars)
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
    cur = await storage.aql().execute(aql, bind_vars=bind_vars)
    await cur.close(ignore_missing=True)
