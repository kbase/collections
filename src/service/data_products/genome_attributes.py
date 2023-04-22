"""
The genome_attribs data product, which provides geneome attributes for a collection.
"""

from collections import defaultdict

from fastapi import APIRouter, Request, Depends, Query
from pydantic import BaseModel, Field, Extra
import src.common.storage.collection_and_field_names as names
from src.service import app_state
from src.service.app_state_data_structures import CollectionsState, PickleableDependencies
from src.service import errors
from src.service import kb_auth
from src.service import processing_matches
from src.service import models
from src.service import processing_selections
from src.service.data_products.common_functions import (
    get_load_version,
    get_load_ver_from_collection,
    remove_collection_keys,
    query_simple_collection_list,
    count_simple_collection_list,
    mark_data_by_kbase_id,
    remove_marked_subset,
)
from src.service.data_products.common_models import (
    DataProductSpec,
    DBCollection,
    QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE,
    QUERY_VALIDATOR_LIMIT,
    QUERY_COUNT,
    QUERY_MATCH_ID,
    QUERY_MATCH_MARK,
    QUERY_SELECTION_ID,
    QUERY_SELECTION_MARK,
)
from src.service.http_bearer import KBaseHTTPBearer
from src.service.routes_common import PATH_VALIDATOR_COLLECTION_ID
from src.service.storage_arango import ArangoStorage, remove_arango_keys
from src.service.timestamp import now_epoch_millis
from typing import Any, Callable, Annotated

# Implementation note - we know FLD_KBASE_ID is unique per collection id /
# load version combination since the loader uses those 3 fields as the arango _key

ID = "genome_attribs"

_ROUTER = APIRouter(tags=["Genome Attributes"], prefix=f"/{ID}")

_MATCH_ID_PREFIX = "m_"
_SELECTION_ID_PREFIX = "s_"


class GenomeAttribsSpec(DataProductSpec):

    async def delete_match(self, storage: ArangoStorage, internal_match_id: str):
        """
        Delete genome attribute match data.

        storage - the storage system
        internal_match_id - the match to delete.
        """
        await remove_marked_subset(
            storage, names.COLL_GENOME_ATTRIBS, _MATCH_ID_PREFIX + internal_match_id)

    async def delete_selection(self, storage: ArangoStorage, internal_selection_id: str):
        """
        Delete genome attribute selection data.

        storage - the storage system
        internal_selection_id - the selection to delete.
        """
        await remove_marked_subset(
            storage, names.COLL_GENOME_ATTRIBS, _SELECTION_ID_PREFIX + internal_selection_id)

    async def apply_selection(self,
        deps: PickleableDependencies,
        storage: ArangoStorage,
        selection: models.InternalSelection,
        collection: models.SavedCollection,
    ):
        """
        Mark selections in genome attribute data.

        deps - the system dependencies
        storage - the storage system.
        selection - the selection to apply.
        collection - the collection to which the selection is attached.
        """
        load_ver = {dp.product: dp.version for dp in collection.data_products}[ID]
        missed = await mark_data_by_kbase_id(
            storage,
            names.COLL_GENOME_ATTRIBS,
            collection.id,
            load_ver,
            selection.selection_ids,
            _SELECTION_ID_PREFIX + selection.internal_selection_id,
        )
        state = models.ProcessState.FAILED if missed else models.ProcessState.COMPLETE
        await storage.update_selection_state(
            selection.internal_selection_id, state, deps.get_epoch_ms(), missed)

    async def get_upas_for_selection(
        self,
        storage: ArangoStorage,
        collection: models.SavedCollection,
        internal_selection_id: str,
    ) -> tuple[dict[str, list[str]], int]:
        """
        Get the workspace UPAs for data in this data product associated with a selection.

        storage - the storage system containing the data.
        collection - the collection containing the selection.
        internal_selection_id - the internal selection ID to use to find selection documents.

        Returns a tuple of
            * A mapping of workspace type to the list of UPAs for that type in the selection
            * The total number of data items processed. Under normal conditions this should
              be equal to the number of UPAs for each type.
        """
        count = [0]
        upamap = defaultdict(list)
        def add_upas(doc: dict[str, Any]):
            count[0] += 1
            types = doc.get(names.FLD_UPA_MAP)
            if not types:
                # maybe throw an error? Means the loader is messed up, unless there really is no
                # external data. Maybe absent field signifies that?
                return
            for type_, upa in types.items():
                upamap[type_].append(upa)

        await process_subset_documents(
            storage,
            collection,
            internal_selection_id,
            models.SubsetType.SELECTION,
            add_upas,
            [names.FLD_UPA_MAP])
        return dict(upamap), count[0]


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
                    names.FLD_KBASE_ID,
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
    doc.pop(names.FLD_UPA_MAP, None)
    return doc


class AttributeName(BaseModel):
    name: str = Field(
        example=names.FLD_KBASE_ID,
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
        example=[{names.FLD_KBASE_ID: "assigned_kbase_genome_id"}],
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
        + "version; in the latter case service administration permissions are required.\n\n"
        + "When creating selections from genome attributes, use the "
        + f"`{names.FLD_KBASE_ID}` field values as input.")
async def get_genome_attributes(
    r: Request,
    collection_id: str = PATH_VALIDATOR_COLLECTION_ID,
    sort_on: str = Query(
        default=names.FLD_KBASE_ID,
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
    limit: int = QUERY_VALIDATOR_LIMIT,
    output_table: bool = Query(
        default=True,
        description="Whether to return the data in table form or dictionary list form"
    ),
    count: bool = QUERY_COUNT,
    match_id: str | None = QUERY_MATCH_ID,
    # TODO FEATURE support a choice of AND or OR for matches & selections
    match_mark: bool = Query(
        default=False,
        description="Whether to mark matched rows rather than filter based on the match ID. "
            + "Matched rows will be indicated by a true value in the special field "
            + f"`{names.FLD_GENOME_ATTRIBS_MATCHED}`."
    ),
    selection_id: str | None = QUERY_SELECTION_ID,
    selection_mark: bool = Query(
        default=False,
        description="Whether to mark selected rows rather than filter based on the selection ID. "
            + "Selected rows will be indicated by a true value in the special field "
            + f"`{names.FLD_GENOME_ATTRIBS_SELECTED}`."
    ),
    load_ver_override: Annotated[str | None, QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE] = None,
    user: kb_auth.KBaseUser = Depends(_OPT_AUTH)
):
    # sorting only works here since we expect the largest collection to be ~300K records and
    # we have a max limit of 1000, which means sorting is O(n log2 1000).
    # Otherwise we need indexes for every sort
    appstate = app_state.get_app_state(r)
    store = appstate.arangostorage
    internal_match_id, internal_selection_id = None, None
    if match_id or selection_id:
        coll = await store.get_collection_active(collection_id)
        load_ver = get_load_ver_from_collection(coll, ID)
    else:
        load_ver = await get_load_version(store, collection_id, ID, load_ver_override, user)
    if match_id:
        internal_match_id = await _get_internal_match_id(appstate, user, coll, match_id)
    if selection_id:
        internal_sel = await processing_selections.get_selection_full(
            appstate, selection_id, require_complete=True, require_collection=coll)
        internal_selection_id = internal_sel.internal_selection_id
    if count:
        return await _count(
            store,
            collection_id,
            load_ver,
            internal_match_id if not match_mark else None,
            internal_selection_id if not selection_mark else None,
        )
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
            internal_selection_id,
            selection_mark,
        ) 


async def _get_internal_match_id(
    appstate: CollectionsState,
    user: kb_auth.KBaseUser,
    coll: models.SavedCollection,
    match_id: str
) -> str:
    if not user:
        raise errors.UnauthorizedError("Authentication is required if a match ID is supplied")
    match = await processing_matches.get_match_full(
        appstate,
        match_id,
        user,
        require_complete=True,
        require_collection=coll
    )
    return match.internal_match_id


def _query_acceptor(
    data: list[dict[str, Any]],
    last: list[dict[str, Any]],
    doc: dict[str, Any],
    output_table: bool
):
    last[0] = doc
    if output_table:
        data.append([doc[k] for k in sorted(_remove_keys(doc))])
    else:
        data.append({k: doc[k] for k in sorted(_remove_keys(doc))})


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
    internal_selection_id: str | None,
    selection_mark: bool,
):
    data = []
    last = [None]
    await query_simple_collection_list(
        store,
        names.COLL_GENOME_ATTRIBS,
        lambda doc: _query_acceptor(data, last, doc, output_table),
        collection_id,
        load_ver,
        sort_on,
        sort_descending=sort_desc,
        skip=skip,
        limit=limit,
        internal_match_id=_prefix_id(_MATCH_ID_PREFIX, internal_match_id),
        match_mark=match_mark,
        match_field=names.FLD_GENOME_ATTRIBS_MATCHED,
        internal_selection_id=_prefix_id(_SELECTION_ID_PREFIX, internal_selection_id),
        selection_mark=selection_mark,
        selection_field=names.FLD_GENOME_ATTRIBS_SELECTED,
    )
    # Sort everything since we can't necessarily rely on arango, the client, or the loader
    # to have the same insertion order for the dicts
    # If we want a specific order the loader should stick a keys doc or something into arango
    # and we order by that
    fields = []
    if last[0]:
        if sort_on not in last[0]: 
            raise errors.IllegalParameterError(
                f"No such field for collection {collection_id} load version {load_ver}: {sort_on}")
        fields = [{"name": k} for k in sorted(last[0])]
    if output_table:
        return {_FLD_SKIP: skip, _FLD_LIMIT: limit, "fields": fields, "table": data}
    else:
        return {_FLD_SKIP: skip, _FLD_LIMIT: limit, "data": data}


def _prefix_id(prefix: str, id_: str | None) -> str | None:
    return prefix + id_ if id_ else None


async def _count(
    store: ArangoStorage,
    collection_id: str,
    load_ver: str,
    internal_match_id: str | None,
    internal_selection_id: str | None,
):
    # for now this method doesn't do much. One we have some filtering implemented
    # it'll need to take that into account.
    count = await count_simple_collection_list(
        store,
        names.COLL_GENOME_ATTRIBS,
        collection_id,
        load_ver,
        internal_match_id=_prefix_id(_MATCH_ID_PREFIX, internal_match_id),
        internal_selection_id=_prefix_id(_SELECTION_ID_PREFIX, internal_selection_id),
    )
    return {_FLD_SKIP: 0, _FLD_LIMIT: 0, "count": count}


async def perform_gtdb_lineage_match(
    internal_match_id: str,
    storage: ArangoStorage,
    lineages: set[str],
    truncated: bool
):
    """
    Add an internal match ID to genome records in the attributes table that match a set of
    GTDB lineages.

    internal_match_id - the ID of the match.
    storage - the storage system containing the match and the genome attribute records.
    lineages - the GTDB lineage strings to match against the genome attributes.
    truncated - whether the lineages have been truncated, and therefore do not represent the full
        lineage.
    """
    match = await storage.get_match_by_internal_id(internal_match_id)
    # use version number to avoid race conditions with activating collections
    coll = await storage.get_collection_version_by_num(match.collection_id, match.collection_ver)
    load_ver = {dp.product: dp.version for dp in coll.data_products}[ID]
    if not truncated:
        await _mark_gtdb_matches_IN_strategy(
            storage, coll.id, load_ver, lineages, match.internal_match_id
        )
    else:
        await _mark_gtdb_matches_STARTS_WITH_strategy(
            storage, coll.id, load_ver, lineages, match.internal_match_id
        )


async def _mark_gtdb_matches_IN_strategy(
    storage: ArangoStorage,
    collection_id: str,
    load_ver: str,
    lineages: set[str],
    internal_match_id: str
):
    # may need to batch this if lineages is too big
    # retries?
    mtch = names.FLD_MATCHES_SELECTIONS
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
            RETURN KEEP(updated, "{names.FLD_KBASE_ID}")
        """
    bind_vars = {
        f"@{_FLD_COL_NAME}": names.COLL_GENOME_ATTRIBS,
        _FLD_COL_ID: collection_id,
        _FLD_COL_LV: load_ver,
        "lineages": list(lineages),
        "internal_match_id": _MATCH_ID_PREFIX + internal_match_id,
    }
    await _mark_gtdb_matches_complete(storage, aql, bind_vars, internal_match_id)


async def _mark_gtdb_matches_complete(
    storage: ArangoStorage,
    aql: str,
    bind_vars: dict[str, Any],
    internal_match_id: str
):
    cur = await storage.execute_aql(aql, bind_vars=bind_vars)
    genome_ids = []
    try:
        async for d in cur:
            genome_ids.append(d[names.FLD_KBASE_ID])
    finally:
        await cur.close(ignore_missing=True)
    await storage.update_match_state(
        internal_match_id, models.ProcessState.COMPLETE, now_epoch_millis(), genome_ids
    )


async def _mark_gtdb_matches_STARTS_WITH_strategy(
    storage: ArangoStorage,
    collection_id: str,
    load_ver: str,
    lineages: set[str],
    internal_match_id: str
):
    # this almost certainly needs to be batched, but let's write it stupid for now and improve
    # later
    # could also probably DRY up this and the above method
    # retries?
    mtch = names.FLD_MATCHES_SELECTIONS
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
            RETURN KEEP(updated, "{names.FLD_KBASE_ID}")
        """
    bind_vars = {
        f"@{_FLD_COL_NAME}": names.COLL_GENOME_ATTRIBS,
        _FLD_COL_ID: collection_id,
        _FLD_COL_LV: load_ver,
        "internal_match_id": _MATCH_ID_PREFIX + internal_match_id,
    }
    for i, lin in enumerate(lineages):
        bind_vars[f"linbottom{i}"] = lin
        # weird stuff could happen if the last character in the string is below a non-printable
        # character, but that seems pretty edgy. Don't worry about it for now
        # Famous last words...
        bind_vars[f"lintop{i}"] = lin[:-1] + chr(ord(lin[-1]) + 1)
    await _mark_gtdb_matches_complete(storage, aql, bind_vars, internal_match_id)


async def process_subset_documents(
    storage: ArangoStorage,
    collection: models.SavedCollection,
    internal_id: str,
    type_: models.SubsetType,
    acceptor: Callable[[dict[str, Any]], None],
    fields: list[str] | None = None,
) -> None:
    """
    Iterate through the documents for a subset, passing them to an acceptor fuction for processing.

    storage - the storage system containing the data.
    collection - the collection containing the subset.
    internal_id - the internal subset ID to use to find subset documents.
    type_ - the type of the subset.
    acceptor - the function that will accept the documents.
    fields - which fields are required from the database documents. Fewer fields means less
        bandwidth consumed.
    """
    load_ver = {d.product: d.version for d in collection.data_products}.get(ID)
    if not load_ver:
        raise ValueError(f"The collection does not have a {ID} data product")
    prefix = _MATCH_ID_PREFIX if type_ == models.SubsetType.MATCH else _SELECTION_ID_PREFIX
    bind_vars = {
        f"@{_FLD_COL_NAME}": names.COLL_GENOME_ATTRIBS,
        _FLD_COL_ID: collection.id,
        _FLD_COL_LV: load_ver,
        "internal_id": prefix + internal_id,
    }
    aql = f"""
    FOR d IN @@{_FLD_COL_NAME}
        FILTER d.{names.FLD_COLLECTION_ID} == @{_FLD_COL_ID}
        FILTER d.{names.FLD_LOAD_VERSION} == @{_FLD_COL_LV}
        FILTER @internal_id IN d.{names.FLD_MATCHES_SELECTIONS}
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

    cur = await storage.execute_aql(aql, bind_vars=bind_vars)
    try:
        async for d in cur:
            acceptor(d)
    finally:
        await cur.close(ignore_missing=True)
