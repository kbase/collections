"""
The genome_attribs data product, which provides genome attributes for a collection.
"""

from collections import defaultdict
import logging
from typing import Any, Callable, Annotated

from fastapi import APIRouter, Request, Depends, Query

import src.common.storage.collection_and_field_names as names
from src.common.product_models import columnar_attribs_common_models as col_models
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
    query_table,
    get_collection_singleton_from_db,
)
from src.service.data_products import common_models
from src.service.data_products.data_product_processing import (
    MATCH_ID_PREFIX,
    SELECTION_ID_PREFIX,
)
from src.service.data_products.table_models import TableAttributes
from src.service.filtering import analyzers
from src.service.filtering.filters import FilterSet
from src.service.http_bearer import KBaseHTTPBearer
from src.service.processing import SubsetSpecification
from src.service.routes_common import PATH_VALIDATOR_COLLECTION_ID
from src.service.storage_arango import ArangoStorage, remove_arango_keys
from src.service.timestamp import now_epoch_millis

# Implementation note - we know FLD_KBASE_ID is unique per collection id /
# load version combination since the loader uses those 3 fields as the arango _key

ID = names.GENOME_ATTRIBS_PRODUCT_ID

_ROUTER = APIRouter(tags=["Genome Attributes"], prefix=f"/{ID}")


class GenomeAttribsSpec(common_models.DataProductSpec):

    async def delete_match(self, storage: ArangoStorage, internal_match_id: str):
        """
        Delete genome attribute match data.

        storage - the storage system
        internal_match_id - the match to delete.
        """
        await remove_marked_subset(
            storage, names.COLL_GENOME_ATTRIBS, MATCH_ID_PREFIX + internal_match_id)

    async def delete_selection(self, storage: ArangoStorage, internal_selection_id: str):
        """
        Delete genome attribute selection data.

        storage - the storage system
        internal_selection_id - the selection to delete.
        """
        await remove_marked_subset(
            storage, names.COLL_GENOME_ATTRIBS, SELECTION_ID_PREFIX + internal_selection_id)

    async def apply_match(self,
        deps: PickleableDependencies,
        storage: ArangoStorage,
        collection: models.SavedCollection,
        internal_match_id: str,
        kbase_ids: list[str],
    ):
        """
        Mark matches in genome attribute data and update the match state to complete.
        If any `kbase_ids` don't exist in the data, the match state is marked as failed.

        deps - the system dependencies
        storage - the storage system.
        collection - the collection to which the selection is attached.
        internal_match_id - the internal ID of the match
        kbase_ids - the matching kbase IDs.
        """
        load_ver = {dp.product: dp.version for dp in collection.data_products}[ID]
        missed = await mark_data_by_kbase_id(
            storage,
            names.COLL_GENOME_ATTRIBS,
            collection.id,
            load_ver,
            kbase_ids,
            MATCH_ID_PREFIX + internal_match_id,
        )
        if missed:
            logging.getLogger(__name__).warn(
                f"Matching process for match with internal ID {internal_match_id} failed due to "
                + f"{len(missed)} input IDs that did not match kbase IDs. First 10: "
                + f"{missed[0:10]}"
        )
        state = models.ProcessState.FAILED if missed else models.ProcessState.COMPLETE
        await storage.update_match_state(
            internal_match_id, state, deps.get_epoch_ms(), None if missed else kbase_ids)

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
            SELECTION_ID_PREFIX + selection.internal_selection_id,
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
        common_models.DBCollection(
            name=names.COLL_GENOME_ATTRIBS_META,
            indexes=[]  # lookup is by key
        ),
        common_models.DBCollection(
            name=names.COLL_GENOME_ATTRIBS,
            view_required=True,
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


##########################
# Filter handling code
##########################

# might want to move some of this into a shared file at some point, might be reusable for other
# DPs
# Not sure about the meta call, may need to abstract that out somehow

_FILTER_PREFIX = "filter_"


async def _get_filters(
    r: Request,
    coll: models.ActiveCollection,
    load_ver: str,
    load_ver_override: bool,
    count: bool,
    sort_on: str,
    sort_desc: bool,
    filter_conjunction: bool,
    skip: int,
    limit: int,
) -> FilterSet:
    # TODO FILTER docs for filters
    # TODO FILTER doc how to change / update the arangosearch view: CLI -> config in API
    # TODO FILTER match & selection
    filter_query = {}
    for q in r.query_params.keys():
        if q.startswith(_FILTER_PREFIX):
            field = q[len(_FILTER_PREFIX):]
            if len(r.query_params.getlist(q)) > 1:
                raise errors.IllegalParameterError(
                    f"More than one filter specification provided for field {field}")
            filter_query[field] = r.query_params[q]
    if not filter_query:
        return None
    appstate = app_state.get_app_state(r)
    column_meta = await _get_genome_attributes_meta_internal(
        appstate.arangostorage, coll.id, load_ver, load_ver_override)
    view_name = coll.get_data_product(ID).search_view
    if not view_name:
        raise ValueError(f"No search view name configured for collection {coll.id}, "
            + "data product {ID}. Cannot perform filtering operation")
    columns = {c.key: c for c in column_meta.columns}
    if sort_on not in columns:
        raise errors.IllegalParameterError(
            f"No such field for collection {coll.id} load version {load_ver}: {sort_on}")
    fs = FilterSet(
        view_name,
        coll.id,
        load_ver,
        count=count,
        sort_on=sort_on,
        sort_descending=sort_desc,
        conjunction=filter_conjunction,
        skip=skip,
        limit=limit
    )
    for field, querystring in filter_query.items():
        if field not in columns:
            raise errors.IllegalParameterError(f"No such filter field: {field}")
        column = columns[field]
        fs.append(
            field,
            column.type,
            querystring,
            analyzers.get_analyzer(column.filter_strategy),
            column.filter_strategy
        )
    return fs


##########################
# End filter handling code
##########################


_FLD_COL_ID = "colid"
_FLD_COL_NAME = "colname"
_FLD_COL_LV = "colload"
_FLD_SORT = "sort"
_FLD_SORT_DIR = "sortdir"
_FLD_SKIP = "skip"
_FLD_LIMIT = "limit"
_FLD_COUNT = "count"


@_ROUTER.get(
    "/meta",
    response_model=col_models.ColumnarAttributesMeta,
    description=
"""
Get metadata about the genome attributes table including column names, type,
minimum and maximum values, etc.
""")
async def get_genome_attributes_meta(
    r: Request,
    collection_id: str = PATH_VALIDATOR_COLLECTION_ID,
    load_ver_override: common_models.QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE = None,
    user: kb_auth.KBaseUser = Depends(_OPT_AUTH)
    ):
    storage = app_state.get_app_state(r).arangostorage
    _, load_ver = await get_load_version(storage, collection_id, ID, load_ver_override, user)
    return await _get_genome_attributes_meta_internal(
        storage, collection_id, load_ver, load_ver_override)


async def _get_genome_attributes_meta_internal(
    storage: ArangoStorage, collection_id: str, load_ver: str, load_ver_override: bool
) -> col_models.ColumnarAttributesMeta:
    doc = await get_collection_singleton_from_db(
            storage,
            names.COLL_GENOME_ATTRIBS_META,
            collection_id,
            load_ver,
            bool(load_ver_override)
    )
    doc[col_models.FIELD_COLUMNS] = [col_models.AttributesColumn.model_construct(**d)
                                     for d in doc[col_models.FIELD_COLUMNS]]
    return col_models.ColumnarAttributesMeta.model_construct(**remove_collection_keys(doc))


@_ROUTER.get(
    "/",
    response_model=TableAttributes,
    description=
f"""
Get genome attributes for each genome in the collection, which may differ from
collection to collection.

Authentication is not required unless submitting a match ID or overriding the load
version; in the latter case service administration permissions are required.

When creating selections from genome attributes, use the
`{names.FLD_KBASE_ID}` field values as input.
""")
async def get_genome_attributes(
    r: Request,
    collection_id: str = PATH_VALIDATOR_COLLECTION_ID,
    sort_on: common_models.QUERY_VALIDATOR_SORT_ON = names.FLD_KBASE_ID,
    sort_desc: common_models.QUERY_VALIDATOR_SORT_DIRECTION = False,
    skip: common_models.QUERY_VALIDATOR_SKIP = 0,
    limit: common_models.QUERY_VALIDATOR_LIMIT = 1000,
    output_table: common_models.QUERY_VALIDATOR_OUTPUT_TABLE = True,
    count: common_models.QUERY_VALIDATOR_COUNT = False,
    conjunction: Annotated[bool, Query(
        description="Whether to AND (true) or OR (false) filters together."
    )] = True,
    match_id: common_models.QUERY_VALIDATOR_MATCH_ID = None,
    # TODO FEATURE support a choice of AND or OR for matches & selections
    match_mark: common_models.QUERY_VALIDATOR_MATCH_MARK_SAFE = False,
    selection_id: common_models.QUERY_VALIDATOR_SELECTION_ID = None,
    selection_mark: common_models.QUERY_VALIDATOR_SELECTION_MARK_SAFE = False,
    load_ver_override: common_models.QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE = None,
    user: kb_auth.KBaseUser = Depends(_OPT_AUTH)
):
    # sorting only works here since we expect the largest collection to be ~300K records and
    # we have a max limit of 1000, which means sorting is O(n log2 1000).
    # Otherwise we need indexes for every sort
    appstate = app_state.get_app_state(r)
    store = appstate.arangostorage
    lvo = override_load_version(load_ver_override, match_id, selection_id)
    coll, load_ver = await get_load_version(appstate.arangostorage, collection_id, ID, lvo, user)
    match_spec = await _get_match_spec(appstate, user, coll, match_id, match_mark)
    sel_spec = await _get_selection_spec(appstate, coll, selection_id, selection_mark)
    filters = await _get_filters(
        r, coll, load_ver, load_ver_override, count, sort_on, sort_desc, conjunction,
        skip, limit)
    if filters:
        return await _perform_filter(store, filters, output_table)
    if count:
        count = await count_simple_collection_list(  # may want to make some sort of shared builder
            store,
            names.COLL_GENOME_ATTRIBS,
            collection_id,
            load_ver,
            match_spec=match_spec,
            selection_spec=sel_spec,
        )
        return {_FLD_SKIP: 0, _FLD_LIMIT: 0, _FLD_COUNT: count}
    else:
        res = await query_table(
            store,
            names.COLL_GENOME_ATTRIBS,
            collection_id,
            load_ver,
            sort_on,
            sort_descending=sort_desc,
            skip=skip,
            limit=limit,
            output_table=output_table,
            match_spec=match_spec,
            selection_spec=sel_spec,
            document_mutator=_remove_keys
        )
        return {
            _FLD_SKIP: res.skip,
            _FLD_LIMIT: res.limit,
            "fields": res.fields,
            "table": res.table,
            "data": res.data
        }


async def _get_match_spec(
    appstate: CollectionsState,
    user: kb_auth.KBaseUser,
    coll: models.SavedCollection,
    match_id: str,
    match_mark: bool,
) -> SubsetSpecification:
    if not match_id:
        return SubsetSpecification()
    if not user:
        raise errors.UnauthorizedError("Authentication is required if a match ID is supplied")
    match_ = await processing_matches.get_match_full(
        appstate,
        match_id,
        user,
        require_complete=True,
        require_collection=coll
    )
    return SubsetSpecification(
        internal_subset_id=match_.internal_match_id, mark_only=match_mark, prefix=MATCH_ID_PREFIX)


async def _get_selection_spec(
    appstate: CollectionsState,
    coll: models.SavedCollection,
    selection_id: str,
    selection_mark: bool,
) -> SubsetSpecification:
    if not selection_id:
        return SubsetSpecification()
    internal_sel = await processing_selections.get_selection_full(
            appstate, selection_id, require_complete=True, require_collection=coll)
    return SubsetSpecification(
        internal_subset_id=internal_sel.internal_selection_id,
        mark_only=selection_mark,
        prefix=SELECTION_ID_PREFIX
    )


async def _perform_filter(store: ArangoStorage, filters: FilterSet, output_table: bool):
    aql, bind_vars = filters.to_arangosearch_aql()
    cur = await store.execute_aql(aql, bind_vars=bind_vars)
    try:
        if filters.count:
            count = await cur.next()
            return {
                _FLD_SKIP: 0,
                _FLD_LIMIT: 0,
                _FLD_COUNT: count
            }
        else:
            data = []
            doc = None
            async for doc in cur:
                # ?ODO FiLTERS this code is pretty similar to the query_table code.
                # See if it can be merged once everything's functional
                doc = _remove_keys(doc)
                if output_table:
                    data.append([doc[k] for k in sorted(doc)])
                else:
                    data.append({k: doc[k] for k in sorted(doc)})
            fields = []
            if doc:
                fields = [{"name": k} for k in sorted(doc)]
            return {
                _FLD_SKIP: filters.skip,
                _FLD_LIMIT: filters.limit,
                "fields": fields,
                "table" if output_table else "data": data,
            }
    finally:
        await cur.close(ignore_missing=True)


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
        "internal_match_id": MATCH_ID_PREFIX + internal_match_id,
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
        "internal_match_id": MATCH_ID_PREFIX + internal_match_id,
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
    Iterate through the documents for a subset, passing them to an acceptor function for processing.

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
    prefix = MATCH_ID_PREFIX if type_ == models.SubsetType.MATCH else SELECTION_ID_PREFIX
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
