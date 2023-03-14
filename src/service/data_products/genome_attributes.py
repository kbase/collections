"""
The genome_attribs data product, which provides geneome attributes for a collection.
"""

from fastapi import APIRouter, Request, Depends, Query
from pydantic import BaseModel, Field, Extra
from src.common.gtdb_lineage import GTDBLineage, GTDBRank
import src.common.storage.collection_and_field_names as names
from src.service import app_state
from src.service import errors
from src.service import kb_auth
from src.service import match_retrieval
from src.service import models
from src.service.data_products.common_functions import (
    get_load_version,
    get_load_ver_from_collection,
    remove_collection_keys
)
from src.service.data_products.common_models import (
    DataProductSpec,
    DBCollection,
    QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE,
)
from src.service.http_bearer import KBaseHTTPBearer
from src.service.routes_common import PATH_VALIDATOR_COLLECTION_ID
from src.service.storage_arango import ArangoStorage, remove_arango_keys
from src.service.timestamp import now_epoch_millis
from src.service.workspace_wrapper import WorkspaceWrapper
from typing import Any, Callable

# Implementation note - we know FLD_GENOME_ATTRIBS_KBASE_GENOME_ID is unique per collection id /
# load version combination since the loader uses those 3 fields as the arango _key

ID = "genome_attribs"

_ROUTER = APIRouter(tags=["Genome Attributes"], prefix=f"/{ID}")


class GenomeAttribsSpec(DataProductSpec):

    async def delete_match(self, storage: ArangoStorage, internal_match_id: str):
        """
        Delete genome attribute match data.

        storage - the storage system
        internal_match_id - the match to delete.
        """
        await delete_match(storage, internal_match_id)

    async def apply_selection(self, storage: ArangoStorage, internal_selection_id: str):
        """
        Mark selections in genome attribute data.

        storage - the storage system.
        internal_selection_id - the selection to apply.
        """
        await mark_selections(storage, internal_selection_id)


GENOME_ATTRIBS_SPEC = GenomeAttribsSpec(
    data_product=ID,
    router=_ROUTER,
    db_collections=[
        DBCollection(
            name=names.COLL_GENOME_ATTRIBS,
            indexes=[
                [
                    names.FLD_COLLECTION_ID,
                    names.FLD_LOAD_VERSION,
                    names.FLD_GENOME_ATTRIBS_KBASE_GENOME_ID,
                    # Since this is the default sort option (see below), we specify an index
                    # for fast sorts since every time the user hits the UI for the first time
                    # or without specifying a sort order it'll sort on this field
                ],
                [
                    names.FLD_COLLECTION_ID,
                    names.FLD_LOAD_VERSION,
                    names.FLD_GENOME_ATTRIBS_GTDB_LINEAGE,
                    # for matching on lineage
                ],
                [
                    names.FLD_COLLECTION_ID,
                    names.FLD_LOAD_VERSION,
                    # https://www.arangodb.com/docs/stable/indexing-index-basics.html#indexing-array-values
                    names.FLD_GENOME_ATTRIBS_MATCHES + "[*]",
                    names.FLD_GENOME_ATTRIBS_KBASE_GENOME_ID,
                    # for finding matches, and optionally a default sort on the genome ID
                ],
                [names.FLD_GENOME_ATTRIBS_MATCHES + "[*]"]  # for match deletion
            ]
        )
    ]
)


_OPT_AUTH = KBaseHTTPBearer(optional=True)

def _remove_keys(doc):
    doc = remove_collection_keys(remove_arango_keys(doc))
    doc.pop(names.FLD_GENOME_ATTRIBS_MATCHES, None)
    doc.pop(names.FLD_GENOME_ATTRIBS_SELECTIONS, None)
    return doc


class AttributeName(BaseModel):
    name: str = Field(
        example=names.FLD_GENOME_ATTRIBS_KBASE_GENOME_ID,
        description="The name of an attribute"
    )

class GenomeAttributes(BaseModel, extra=Extra.allow):
    """
    Attributes for a set of genomes. Either `fields` and `table` are returned, `data` is
    returned, or `count` is returned.
    The set of available attributes may be different for different collections.
    """
    skip: int = Field(example=0, description="The number of records that were skipped.")
    limit: int = Field(
        example=1000,
        description="The maximum number of results that could be returned. "
            + "0 and meaningless if `count` is specified"
    )
    # may need to return fields with data in the future if we add more info to fields
    fields: list[AttributeName] | None = Field(
        description="The name for each column in the attribute table."
    )
    table: list[list[Any]] | None = Field(
        example=[["my_genome_name"]],
        description="The attributes in an NxM table. Each column's name is available at the "
            + "corresponding index in the fields parameter. Each inner list is a row in the "
            + "table with each entry being the entry for that column."
    )
    data: list[dict[str, Any]] | None = Field(
        example=[{names.FLD_GENOME_ATTRIBS_KBASE_GENOME_ID: "assigned_kbase_genome_id"}],
        description="The attributes as a list of dictionaries."
    )
    count: int | None = Field(
        example=42,
        description="The number of attribute records that match the query.",
    )

_FLD_COL_ID = "colid"
_FLD_COL_NAME = "colname"
_FLD_COL_LV = "colload"
_FLD_SORT = "sort"
_FLD_SORT_DIR = "sortdir"
_FLD_SKIP = "skip"
_FLD_LIMIT = "limit"


# At some point we're going to want to filter/sort on fields. We may want a list of fields
# somewhere to check input fields are ok... but really we could just fetch the first document
# in the collection and check the fields 
@_ROUTER.get(
    "/",
    response_model=GenomeAttributes,
    description="Get genome attributes for each genome in the collection, which may differ from "
        + "collection to collection.\n\n "
        + "Authentication is not required unless submitting a match ID or overriding the load "
        + " version; in the latter case service administration permissions are required.")
async def get_genome_attributes(
    r: Request,
    collection_id: str = PATH_VALIDATOR_COLLECTION_ID,
    sort_on: str = Query(
        default=names.FLD_GENOME_ATTRIBS_KBASE_GENOME_ID,
        example="genome_size",
        description="The field to sort on."
    ),
    sort_desc: bool = Query(
        default=False,
        description="Whether to sort in descending order rather than ascending"
    ),
    skip: int = Query(
        default=0,
        ge=0,
        le=10000,
        example=1000,
        description="The number of records to skip"
    ),
    limit: int = Query(
        default=1000,
        ge=1,
        le=1000,
        example=1000,
        description="The maximum number of results"
    ),
    output_table: bool = Query(
        default=True,
        description="Whether to return the data in table form or dictionary list form"
    ),
    count: bool = Query(
        default = False,
        description="Whether to return the number of records that match the query rather than "
            + "the records themselves. Paging parameters are ignored."
    ),
    match_id: str | None = Query(
        default = None,
        description="A match ID to set the view to the match rather than "
            + "the entire collection. Authentication is required. Note that if a match ID is "
            + "set, any load version override is ignored."),
            # matches are against a specific load version, so...
    match_mark: bool = Query(
        default=False,
        description="Whether to mark matched rows rather than filter based on the match ID. "
            + "Matched rows will be indicated by a true value in the special field "
            + f"`{names.FLD_GENOME_ATTRIBS_MATCHED}`. Has no effect if 'count' is true."
    ),
    load_ver_override: str | None = QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE,
    user: kb_auth.KBaseUser = Depends(_OPT_AUTH)
):
    # sorting only works here since we expect the largest collection to be ~300K records and
    # we have a max limit of 1000, which means sorting is O(n log2 1000).
    # Otherwise we need indexes for every sort
    store = app_state.get_app_state(r).arangostorage
    internal_match_id = None
    if match_id:
        if not user:
            raise errors.UnauthorizedError("Authentication is required if a match ID is supplied")
        coll = await store.get_collection_active(collection_id)
        load_ver = get_load_ver_from_collection(coll, ID)
        ws = app_state.get_app_state(r).get_workspace_client(user.token)
        match = await match_retrieval.get_match_full(
            app_state.get_app_state(r),
            match_id,
            user,
            require_complete=True,
            require_collection=coll
        )
        internal_match_id = match.internal_match_id
    else:
        load_ver = await get_load_version(store, collection_id, ID, load_ver_override, user)
    if count:
        return await _count(store, collection_id, load_ver, internal_match_id)
    else:
        return await _query(
            store,
            collection_id,
            load_ver,
            sort_on,
            sort_desc,
            skip,
            limit,
            output_table,
            internal_match_id,
            match_mark,
        ) 


async def _query(
    # ew. too many args
    store: ArangoStorage,
    collection_id: str,
    load_ver: str,
    sort_on: str,
    sort_desc: bool,
    skip: int,
    limit: int,
    output_table: bool,
    internal_match_id: str | None,
    match_mark: bool,
):
    bind_vars = {
        f"@{_FLD_COL_NAME}": names.COLL_GENOME_ATTRIBS,
        _FLD_COL_ID: collection_id,
        _FLD_COL_LV: load_ver,
        _FLD_SORT: sort_on,
        _FLD_SORT_DIR: "DESC" if sort_desc else "ASC",
        _FLD_SKIP: skip,
        _FLD_LIMIT: limit
    }
    aql = f"""
    FOR d IN @@{_FLD_COL_NAME}
        FILTER d.{names.FLD_COLLECTION_ID} == @{_FLD_COL_ID}
        FILTER d.{names.FLD_LOAD_VERSION} == @{_FLD_COL_LV}
    """
    if internal_match_id and not match_mark:
        bind_vars["internal_match_id"] = internal_match_id
        aql += f"""
            FILTER @internal_match_id IN d.{names.FLD_GENOME_ATTRIBS_MATCHES}
        """
    aql += f"""
        SORT d.@{_FLD_SORT} @{_FLD_SORT_DIR}
        LIMIT @{_FLD_SKIP}, @{_FLD_LIMIT}
        RETURN d
    """
    # could get the doc count, but that'll be slower since all docs have to be counted vs. just
    # getting LIMIT docs. YAGNI for now
    cur = await store.aql().execute(aql, bind_vars=bind_vars)
    # Sort everything since we can't necessarily rely on arango, the client, or the loader
    # to have the same insertion order for the dicts
    # If we want a specific order the loader should stick a keys doc or something into arango
    # and we order by that
    data = []
    d = None
    try:
        async for d in cur:
            if internal_match_id:
                d[names.FLD_GENOME_ATTRIBS_MATCHED] = internal_match_id in d[
                    names.FLD_GENOME_ATTRIBS_MATCHES]
            if output_table:
                data.append([d[k] for k in sorted(_remove_keys(d))])
            else:
                data.append({k: d[k] for k in sorted(_remove_keys(d))})
    finally:
        await cur.close(ignore_missing=True)
    fields = []
    if d:
        if sort_on not in d: 
            raise errors.IllegalParameterError(
                f"No such field for collection {collection_id} load version {load_ver}: {sort_on}")
        fields = [{"name": k} for k in sorted(d)]
    if output_table:
        return {_FLD_SKIP: skip, _FLD_LIMIT: limit, "fields": fields, "table": data}
    else:
        return {_FLD_SKIP: skip, _FLD_LIMIT: limit, "data": data}


async def _count(
    store: ArangoStorage,
    collection_id: str,
    load_ver: str,
    internal_match_id: str | None
):
    # for now this method doesn't do much. One we have some filtering implemented
    # it'll need to take that into account.

    bind_vars = {
        f"@{_FLD_COL_NAME}": names.COLL_GENOME_ATTRIBS,
        _FLD_COL_ID: collection_id,
        _FLD_COL_LV: load_ver,
    }
    aql = f"""
    FOR d IN @@{_FLD_COL_NAME}
        FILTER d.{names.FLD_COLLECTION_ID} == @{_FLD_COL_ID}
        FILTER d.{names.FLD_LOAD_VERSION} == @{_FLD_COL_LV}
    """
    if internal_match_id:
        bind_vars["internal_match_id"] = internal_match_id
        aql += f"""
            FILTER @internal_match_id IN d.{names.FLD_GENOME_ATTRIBS_MATCHES}
        """
    aql += f"""
        COLLECT WITH COUNT INTO length
        RETURN length
    """
    cur = await store.aql().execute(aql, bind_vars=bind_vars)
    try:
        return {_FLD_SKIP: 0, _FLD_LIMIT: 0, "count": await cur.next()}
    finally:
        await cur.close(ignore_missing=True)


async def perform_gtdb_lineage_match(
    match_id: str,
    storage: ArangoStorage,
    lineages: list[str],
    rank: GTDBRank
):
    """
    Add an internal match ID to genome records in the attributes table that match a set of
    GTDB lineages.

    match_id - the ID of the match.
    storage - the storage system containing the match and the genome attribute records.
    lineages - the GTDB lineage strings to match against the genome attributes
    rank - the rank at which to match. This effectively truncates the lineage strings when
        matching.
    """
    # Could save some bandwidth here buy adding a method to just get the internal ID
    # Microoptimization, wait until it's a problem
    match = await storage.get_match_full(match_id)
    # use version number to avoid race conditions with activating collections
    coll = await storage.get_collection_version_by_num(match.collection_id, match.collection_ver)
    load_ver = {dp.product: dp.version for dp in coll.data_products}[ID]
    filtered_lineages = set()  # remove duplicates
    for lin in lineages:
        # may need to catch errors here and report with object UPA or something
        # or parse lineages prior to starting the process, that's probably better
        lineage = GTDBLineage(lin, force_complete=False).truncate_to_rank(rank)
        if lineage:
            filtered_lineages.add(str(lineage))
    if rank == GTDBRank.SPECIES:
        await _mark_gtdb_matches_IN_strategy(
            storage, coll.id, load_ver, filtered_lineages, match.match_id, match.internal_match_id
        )
    else:
        await _mark_gtdb_matches_STARTS_WITH_strategy(
            storage, coll.id, load_ver, filtered_lineages, match.match_id, match.internal_match_id
        )


async def _mark_gtdb_matches_IN_strategy(
    storage: ArangoStorage,
    collection_id: str,
    load_ver: str,
    lineages: set[str],
    match_id: str,
    internal_match_id: str
):
    # may need to batch this if lineages is too big
    # retries?
    mtch = names.FLD_GENOME_ATTRIBS_MATCHES
    aql = f"""
        FOR d IN @@{_FLD_COL_NAME}
            FILTER d.{names.FLD_COLLECTION_ID} == @{_FLD_COL_ID}
            FILTER d.{names.FLD_LOAD_VERSION} == @{_FLD_COL_LV}
            FILTER d.{names.FLD_GENOME_ATTRIBS_GTDB_LINEAGE} IN @lineages
            UPDATE d WITH {{
                {mtch}: APPEND(d.{mtch}, [@internal_match_id], true)
            }} IN @@{_FLD_COL_NAME}
            OPTIONS {{exclusive: true}}
            LET updated = NEW
            RETURN KEEP(updated, "{names.FLD_GENOME_ATTRIBS_KBASE_GENOME_ID}")
        """
    bind_vars = {
        f"@{_FLD_COL_NAME}": names.COLL_GENOME_ATTRIBS,
        _FLD_COL_ID: collection_id,
        _FLD_COL_LV: load_ver,
        "lineages": list(lineages),
        "internal_match_id": internal_match_id,
    }
    await _mark_gtdb_matches_complete(storage, aql, bind_vars, match_id)


async def _mark_gtdb_matches_complete(
    storage: ArangoStorage,
    aql: str,
    bind_vars: dict[str, Any],
    match_id: str
):
    cur = await storage.aql().execute(aql, bind_vars=bind_vars)
    genome_ids = []
    try:
        async for d in cur:
            genome_ids.append(d[names.FLD_GENOME_ATTRIBS_KBASE_GENOME_ID])
    finally:
        await cur.close(ignore_missing=True)
    await storage.update_match_state(
        match_id, models.ProcessState.COMPLETE, now_epoch_millis(), genome_ids
    )


async def _mark_gtdb_matches_STARTS_WITH_strategy(
    storage: ArangoStorage,
    collection_id: str,
    load_ver: str,
    lineages: set[str],
    match_id: str,
    internal_match_id: str
):
    # this almost certainly needs to be batched, but let's write it stupid for now and improve
    # later
    # could also probably DRY up this and the above method
    # retries?
    mtch = names.FLD_GENOME_ATTRIBS_MATCHES
    lin = names.FLD_GENOME_ATTRIBS_GTDB_LINEAGE
    aql = f"""
        FOR d IN @@{_FLD_COL_NAME}
            FILTER d.{names.FLD_COLLECTION_ID} == @{_FLD_COL_ID}
            FILTER d.{names.FLD_LOAD_VERSION} == @{_FLD_COL_LV}
            FILTER"""
    for i in range(len(lineages)):
        if i != 0:
            aql += "                  "
        aql += f" (d.{lin} >= @linbottom{i} AND d.{lin} < @lintop{i})"
        if i < len(lineages) - 1:
            aql += " OR "
        aql += "\n"
    aql += f"""
            UPDATE d WITH {{
                {mtch}: APPEND(d.{mtch}, [@internal_match_id], true)
            }} IN @@{_FLD_COL_NAME}
            OPTIONS {{exclusive: true}}
            LET updated = NEW
            RETURN KEEP(updated, "{names.FLD_GENOME_ATTRIBS_KBASE_GENOME_ID}")
        """
    bind_vars = {
        f"@{_FLD_COL_NAME}": names.COLL_GENOME_ATTRIBS,
        _FLD_COL_ID: collection_id,
        _FLD_COL_LV: load_ver,
        "internal_match_id": internal_match_id,
    }
    for i, lin in enumerate(lineages):
        bind_vars[f"linbottom{i}"] = lin
        # weird stuff could happen if the last character in the string is below a non-printable
        # character, but that seems pretty edgy. Don't worry about it for now
        # Famous last words...
        bind_vars[f"lintop{i}"] = lin[:-1] + chr(ord(lin[-1]) + 1)
    await _mark_gtdb_matches_complete(storage, aql, bind_vars, match_id)


async def process_match_documents(
    storage: ArangoStorage,
    collection: models.SavedCollection,
    internal_match_id: str,
    acceptor: Callable[[dict[str, Any]], None],
    fields: list[str] | None = None,
) -> None:
    """
    Iterate through the documents for a match, passing them to an acceptor fuction for processing.

    storage - the storage system containing the data.
    collection_id - the ID of the collection the match is against.
    load_version - the load version of the data in the match.
    internal_match_id = the internal match ID to use to find matched documents.
    acceptor - the function that will accept the documents.
    fields - which fields are required from the database documents. Fewer fields means less
        bandwidth consumed.
    """
    load_ver = {d.product: d.version for d in collection.data_products}.get(ID)
    if not load_ver:
        raise ValueError(f"The collection does not have a {ID} data product")
    bind_vars = {
        f"@{_FLD_COL_NAME}": names.COLL_GENOME_ATTRIBS,
        _FLD_COL_ID: collection.id,
        _FLD_COL_LV: load_ver,
        "internal_match_id": internal_match_id,
    }
    aql = f"""
    FOR d IN @@{_FLD_COL_NAME}
        FILTER d.{names.FLD_COLLECTION_ID} == @{_FLD_COL_ID}
        FILTER d.{names.FLD_LOAD_VERSION} == @{_FLD_COL_LV}
        FILTER @internal_match_id IN d.{names.FLD_GENOME_ATTRIBS_MATCHES}
        """
    if fields:
        aql += """
            RETURN KEEP(d, @keep)
        """
        bind_vars["keep"] = fields
    else:    
        aql += """
            RETURN d
        """

    cur = await storage.aql().execute(aql, bind_vars=bind_vars)
    try:
        async for d in cur:
            acceptor(d)
    finally:
        await cur.close(ignore_missing=True)


async def delete_match(storage: ArangoStorage, internal_match_id: str):
    """
    Delete genome attribues match data.

    storage - the storage system
    internal_match_id - the match to delete.
    """
    m = names.FLD_GENOME_ATTRIBS_MATCHES
    bind_vars = {
        f"@{_FLD_COL_NAME}": names.COLL_GENOME_ATTRIBS,
        "internal_match_id": internal_match_id,
    }
    aql = f"""
        FOR d IN @@{_FLD_COL_NAME}
            FILTER @internal_match_id IN d.{m}
            UPDATE d WITH {{
                {m}: REMOVE_VALUE(d.{m}, @internal_match_id)
            }} IN @@{_FLD_COL_NAME}
            OPTIONS {{exclusive: true}}
        """
    cur = await storage.aql().execute(aql, bind_vars=bind_vars)
    await cur.close(ignore_missing=True)


async def mark_selections(storage: ArangoStorage, internal_selection_id: str):
    """
    Mark genome attribute entries that are present in the selection and complete the selection
    process.
    """
    sel = await storage.get_selection_internal(internal_selection_id)
    # use version number to avoid race conditions with activating collections
    coll = await storage.get_collection_version_by_num(sel.collection_id, sel.collection_ver)
    load_ver = {dp.product: dp.version for dp in coll.data_products}[ID]

    # a bit too tricky to DRY this up with gtdb lineage matches above, although they're similar
    # retries?
    selfld = names.FLD_GENOME_ATTRIBS_SELECTIONS
    aql = f"""
        FOR d IN @@{_FLD_COL_NAME}
            FILTER d.{names.FLD_COLLECTION_ID} == @{_FLD_COL_ID}
            FILTER d.{names.FLD_LOAD_VERSION} == @{_FLD_COL_LV}
            FILTER d.{names.FLD_GENOME_ATTRIBS_KBASE_GENOME_ID} IN @genome_ids
            UPDATE d WITH {{
                {selfld}: APPEND(d.{selfld}, [@internal_match_id], true)
            }} IN @@{_FLD_COL_NAME}
            OPTIONS {{exclusive: true}}
            LET updated = NEW
            RETURN KEEP(updated, "{names.FLD_GENOME_ATTRIBS_KBASE_GENOME_ID}")
        """
    bind_vars = {
        f"@{_FLD_COL_NAME}": names.COLL_GENOME_ATTRIBS,
        _FLD_COL_ID: coll.id,
        _FLD_COL_LV: load_ver,
        "genome_ids": sel.selection_ids,
        "internal_match_id": internal_selection_id,
    }
    matched = set()
    cur = await storage.aql().execute(aql, bind_vars=bind_vars)
    try:
        async for d in cur:
            matched.add(d[names.FLD_GENOME_ATTRIBS_KBASE_GENOME_ID])
    finally:
        await cur.close(ignore_missing=True)
    missed = sorted(set(sel.selection_ids) - matched)
    state = models.ProcessState.FAILED if missed else models.ProcessState.COMPLETE
    await storage.update_selection_state(internal_selection_id, state, now_epoch_millis(), missed)
