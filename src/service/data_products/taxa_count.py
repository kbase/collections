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
from src.service import match_retrieval
from src.service import models
from src.service import processing
from src.service.clients.workspace_client import Workspace
from src.service.data_products.common_functions import (
    get_load_version,
    get_load_ver_from_collection,
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
from src.service.workspace_wrapper import WorkspaceWrapper


ID = "taxa_count"

_ROUTER = APIRouter(tags=["Taxa count"], prefix=f"/{ID}")


class TaxaCountSpec(DataProductSpec):

    async def delete_match(self, storage: ArangoStorage, internal_match_id: str):
        """
        Delete taxa count match data.

        storage - the storage system
        internal_match_id - the match to delete.
        """
        await delete_match(storage, internal_match_id)


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
                    names.FLD_INTERNAL_MATCH_ID,
                    names.FLD_TAXA_COUNT_RANK,
                    names.FLD_TAXA_COUNT_COUNT,
                ],
                [names.FLD_INTERNAL_MATCH_ID]  # for deleting match data
            ]
        )
    ]
)

_OPT_AUTH = KBaseHTTPBearer(optional=True)


def ranks_key(collection_id: str, load_ver: str):
    f"""
    Calculate the ranks database key for the {ID} data product.
    """
    return md5_string(f"{collection_id}_{load_ver}")


# modifies in place
def _remove_counts_keys(doc: dict):
    for k in [names.FLD_TAXA_COUNT_RANK,
              names.FLD_COLLECTION_ID,
              names.FLD_LOAD_VERSION,
              names.FLD_INTERNAL_MATCH_ID,
             ]:
        doc.pop(k, None)
    return doc


class Ranks(BaseModel):
    data: list[str] = Field(
        example=["domain", "phylum"],
        description="A list of taxonomy ranks in rank order"
    )


# Note these field names need to match those in src.common.storage.collection_and_field_names
# except for match count
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
    # TODO SELECTION selection count


# this needs to match the match_count field name in TaxaCount above
_MATCH_COUNT = "match_count"


class TaxaCounts(BaseModel):
    """
    The taxa counts data set. Either `data` or `taxa_count_match_state` is returned.
    """
    data: list[TaxaCount] | None
    taxa_count_match_state: models.ProcessState | None = Field(
        example=models.ProcessState.PROCESSING,
        description="The processing state of the match for this data product. This data product "
            + "requires additional processing beyond the primary match."
    )


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
    aql = f"""
        FOR d IN @@{_FLD_COL_ID}
            FILTER d.{names.FLD_ARANGO_KEY} == @{_FLD_KEY}
            RETURN d
    """
    bind_vars = {
        f"@{_FLD_COL_ID}": names.COLL_TAXA_COUNT_RANKS,
        _FLD_KEY: ranks_key(collection_id, load_ver)
    }
    cur = await store.aql().execute(aql, bind_vars=bind_vars, count=True)
    try:
        if cur.count() < 1:
            err = f"No data loaded for {collection_id} collection load version {load_ver}"
            if load_ver_overridden:
                raise errors.NoDataFoundError(err)
            raise ValueError(err)
        # since we're getting a doc by _key > 1 is impossible
        doc = await cur.next()
    finally:
        await cur.close(ignore_missing=True)
    return Ranks(data=doc[names.FLD_TAXA_COUNT_RANKS])


@_ROUTER.get(
    "/counts/{rank}/",
    response_model=TaxaCounts,
    description="Get the taxonomy counts in descending order. At most 20 taxa are returned.\n\n "
        + "Authentication is not required unless overriding the load version, in which case "
        + "service administration permissions are required.")
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
        description="A match ID to set the view to the match rather than "
            + "the entire collection. Note that if a match ID is set, any load version override "
            + "is ignored."),  # matches are against a specific load version, so...
    load_ver_override: str | None = QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE,
    user: kb_auth.KBaseUser = Depends(_OPT_AUTH)
):
    appstate = app_state.get_app_state(r)
    store = appstate.arangostorage
    dp_match = None
    if match_id:
        dp_match, load_ver = await _get_data_product_match(
            appstate, collection_id, match_id, user
        )
        if dp_match.state != models.ProcessState.COMPLETE:
            return TaxaCounts(taxa_count_match_state=dp_match.state)
    else:
        load_ver = await get_load_version(store, collection_id, ID, load_ver_override, user)
    ranks = await get_ranks_from_db(store, collection_id, load_ver, bool(load_ver_override))
    if rank not in ranks.data:
        raise errors.IllegalParameterError(f"Invalid rank: {rank}")
    q = await _query(store, collection_id, load_ver, rank)
    if dp_match:
        matchq = await _query(
            store,
            collection_id,
            load_ver,
            rank,
            dp_match.internal_id
        )
        name, count = names.FLD_TAXA_COUNT_NAME, names.FLD_TAXA_COUNT_COUNT
        mqd = {d[name]: d[count] for d in matchq}
        for d in q:
            d[_MATCH_COUNT] = mqd.get(d[name], 0)
        # For now always sort by the non-match data. See if ppl want sort by match data before implementing
        # something more sophisticated.
    return TaxaCounts(
        data=q,
        taxa_count_match_state=dp_match.state if dp_match else None
    )


async def _get_data_product_match(
    appstate: CollectionsState,
    collection_id: str,
    match_id: str,
    user: kb_auth.KBaseUser
):
    if not user:
        raise errors.UnauthorizedError("Authentication is required if a match ID is supplied")
    coll = await appstate.arangostorage.get_collection_active(collection_id)
    # I'm kind of uncomfortable hard coding this dependency... but it's real so... I dunno.
    # Might need refactoring later once it become more clear how data products should
    # interact.
    if genome_attributes.ID not in {dp.product for dp in coll.data_products}:
        raise errors.InvalidMatchStateError(
            f"Cannot perform a {ID} match when the collection does not have a "
            + f"{genome_attributes.ID} data product")
    load_ver = get_load_ver_from_collection(coll, ID)
    match = await match_retrieval.get_match_full(
        appstate,
        match_id,
        user,
        require_complete=True,
        require_collection=coll
    )
    dp_match = await match_retrieval.get_or_create_data_product_match(
        appstate, match, ID, _process_match
    )
    return dp_match, load_ver


async def _query(
    store: ArangoStorage,
    collection_id: str,
    load_ver: str,
    rank: str,
    internal_match_id: str | None = None,
):
    bind_vars = {
        f"@{_FLD_COL_NAME}": names.COLL_TAXA_COUNT,
        _FLD_COL_ID: collection_id,
        _FLD_COL_LV: load_ver,
        _FLD_COL_RANK: rank,
        "internal_match_id": internal_match_id,
    }
    # will probably want some sort of sort / limit options, but don't get too crazy. Wait for
    # feedback for now
    aql = f"""
        FOR d IN @@{_FLD_COL_NAME}
            FILTER d.{names.FLD_COLLECTION_ID} == @{_FLD_COL_ID}
            FILTER d.{names.FLD_LOAD_VERSION} == @{_FLD_COL_LV}
            FILTER d.{names.FLD_INTERNAL_MATCH_ID} == @internal_match_id
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


async def _process_match(match_id: str, deps: PickleableDependencies, args: list):
    # TODO DOCS document that match processes should run the process regardless of the state
    #      of the match. It is up to the code starting the process to ensure it is correct to
    #      start the match process. As such, the match processes should be idempotent.
    hb = None
    arangoclient = None
    match = None
    try:
        arangoclient, storage = await deps.get_storage()
        match = await storage.get_match_full(match_id)
        async def heartbeat(millis: int):
            await storage.send_data_product_heartbeat(
                match.internal_match_id, ID, models.ProcessType.MATCH, millis)
        hb = processing.Heartbeat(heartbeat, processing.HEARTBEAT_INTERVAL_SEC)
        hb.start()
        
        # use version number to avoid race conditions with activating collections
        coll = await storage.get_collection_version_by_num(
            match.collection_id, match.collection_ver
        )
        load_ver = {dp.product: dp.version for dp in coll.data_products}[ID]
        # Right now this is hard coded to use the GTDB lineage, which is the only choice we
        # have. Might need to expand in future for other count strategies.
        # Maybe a collection parameter, which would be available from the collection data structure
        # given the matcher ID.
        count = GTDBTaxaCount()
        await genome_attributes.process_match_documents(
            storage,
            coll,
            match.internal_match_id,
            lambda doc: count.add(doc[names.FLD_GENOME_ATTRIBS_GTDB_LINEAGE]),
            fields=[names.FLD_GENOME_ATTRIBS_GTDB_LINEAGE]
        )
        docs = [taxa_node_count_to_doc(coll.id, load_ver, tc, match.internal_match_id)
                for tc in count
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
        await _update_match_state(
            storage, match.internal_match_id, models.ProcessState.COMPLETE, deps.get_epoch_ms()
        )
    except Exception as e:
        logging.getLogger(__name__).exception(
            f"Matching process data product {ID} for match {match_id} failed")
        if match:
            await _update_match_state(
               storage, match.internal_match_id, models.ProcessState.FAILED, deps.get_epoch_ms()
            )
            # otherwise not much to do, something went very wrong
    finally:
        if hb:
            hb.stop()
        if arangoclient:
            await arangoclient.close()


async def _update_match_state(
    storage: ArangoStorage,
    internal_match_id: str,
    state: models.ProcessState,
    epoch_ms: int
):
    await storage.update_data_product_process_state(
        internal_match_id, ID, models.ProcessType.MATCH, state, epoch_ms
    )


async def delete_match(storage: ArangoStorage, internal_match_id: str):
    """
    Delete taxa count match data.

    storage - the storage system
    internal_match_id - the match to delete.
    """
    bind_vars = {
        f"@{_FLD_COL_NAME}": names.COLL_TAXA_COUNT,
        "internal_match_id": internal_match_id,
    }
    aql = f"""
        FOR d IN @@{_FLD_COL_NAME}
            FILTER d.{names.FLD_INTERNAL_MATCH_ID} == @internal_match_id
            REMOVE d IN @@{_FLD_COL_NAME}
            OPTIONS {{exclusive: true}}
    """
    cur = await storage.aql().execute(aql, bind_vars=bind_vars)
    await cur.close(ignore_missing=True)
