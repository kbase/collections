"""
Functions common to all data products
"""

import src.common.storage.collection_and_field_names as names
from src.common.storage.db_doc_conversions import (
    collection_load_version_key,
    collection_data_id_key,
)
from src.service import errors
from src.service import models
from src.service import kb_auth
from src.service.storage_arango import ArangoStorage

from typing import Any, Callable, NamedTuple


def override_load_version(
    load_ver_override: str = None, match_id: str = None, selection_id: str = None
) -> str | None:
    """
    Determines whether a load version override should be used for subsequent method calls. Returns
    the provided override if it's present and no match or selection ID is provided.
    """
    return None if match_id or selection_id else load_ver_override


async def get_load_version(
    store: ArangoStorage,
    collection_id: str,
    data_product: str,
    load_ver: str,
    user: kb_auth.KBaseUser,
) -> tuple[models.ActiveCollection | None, str]:
    """
    Get a collection and the load version of a data product given a Collection ID and the ID
    of the data product, optionally allowing an override of the load version if the user is
    a service administrator. If the load version is overridden a collection is not returned
    to allow for providing load versions for data that is not yet active.

    store - the data storage system.
    collection_id - the ID of the Collection from which to retrieve the load version and possibly
        collection object.
    data_product - the ID of the data product from which to retrieve the load version.
    load_ver - an override for the load version. If provided:
        * the user must be a service administrator
        * the collection is not checked for the existence of the data product.
    user - the user. Ignored if load_ver is not provided; must be a service administrator.

    Will throw an error if 
        * the Collection does not have the data product activated.
        * load_ver is provided and user is not a service administrator.
    """
    if load_ver:
        if not user or user.admin_perm != kb_auth.AdminPermission.FULL:
            raise errors.UnauthorizedError(
                "To override the load version a user must be a system administrator")
        return None, load_ver
    ac = await store.get_collection_active(collection_id)
    return ac, _get_load_ver_from_collection(ac, data_product)


def _get_load_ver_from_collection(collection: models.SavedCollection, data_product: str) -> str:
    """
    Get the load version of a data product given a collection and the ID of the data product.
    """
    for dp in collection.data_products:
        if dp.product == data_product:
            return dp.version
    raise errors.NoRegisteredDataProductError(
        f"The {collection.id} collection does not have a {data_product} data product registered.")


def remove_collection_keys(doc: dict):
    """ Removes the collection ID and load version keys from a dictionary **in place**. """
    for k in [names.FLD_COLLECTION_ID, names.FLD_LOAD_VERSION]:
        doc.pop(k, None)
    return doc


async def get_collection_singleton_from_db(
    store: ArangoStorage,
    collection: str,
    collection_id: str,
    load_ver: str,
    no_data_error: bool,
) -> dict[str, Any]:
    """
    Get a document from the database where it is expected that only one instance of that document
    exists per collection load version and the key is calculated by `collection_load_version_key`.
    
    store - the storage system.
    collection - the arango collection containing the document.
    collection_id - the KBase collection containing the document.
    load_ver - the load version of the collection.
    no_data_error - raise a NoDataFoundError (indicating a caller error) instead of a ValueError
        (indicating a problem with the database) if the document isn't found.
    """
    return await _query_collection(
        store,
        collection,
        collection_load_version_key(collection_id, load_ver),
        f"No data loaded for {collection_id} collection load version {load_ver}",
        no_data_error,
    )


async def get_doc_from_collection_by_unique_id(
    store: ArangoStorage,
    collection: str,
    collection_id: str,
    load_ver: str,
    data_id: str,
    data_type: str,
    no_data_error: bool,
) -> dict[str, Any]:
    """
    Get a document from the database where it is expected that only one instance of that document
    exists per collection load version for a specific data ID and the key is calculated by
    `collection_data_id_key`.
    
    store - the storage system.
    collection - the arango collection containing the document.
    collection_id - the KBase collection containing the document.
    load_ver - the load version of the collection.
    data_id - the unique ID of the document in the collection load.
    data_type - the type of the data. This field is used for customizing the error message.
    no_data_error - raise a NoDataFoundError (indicating a caller error) instead of a ValueError
        (indicating a problem with the database) if the document isn't found.
    """
    return await _query_collection(
        store,
        collection,
        collection_data_id_key(collection_id, load_ver, data_id),
        f"No {data_type} found for {collection_id} collection load version "
            + f"{load_ver} ID {data_id}",
        no_data_error,
    )


async def _query_collection(
    store: ArangoStorage,
    collection: str,
    key: str,
    err: str,
    no_data_error: bool,
) -> dict[str, Any]:
    aql = f"""
        FOR d IN @@coll
            FILTER d.{names.FLD_ARANGO_KEY} == @key
            RETURN d
    """
    bind_vars = {
        "@coll": collection,
        "key": key,
    }
    cur = await store.execute_aql(aql, bind_vars=bind_vars, count=True)
    try:
        if cur.count() < 1:
            if no_data_error:
                raise errors.NoDataFoundError(err)
            raise ValueError(err)
        # since we're getting a doc by _key > 1 is impossible
        return await cur.next()
    finally:
        await cur.close(ignore_missing=True)


async def query_simple_collection_list(
    # too many args, ew
    storage: ArangoStorage,
    collection: str,
    acceptor: Callable[[dict[str, Any]], None],
    collection_id: str,
    load_ver: str,
    sort_on: str,
    sort_descending: bool = False,
    skip: int = 0,
    start_after: str = None,
    limit: int = 1000,
    internal_match_id: str | None = None,
    match_process: models.DataProductProcess | None = None,
    match_mark: bool = False,
    match_prefix: str | None = None,
    match_field: str = names.FLD_MATCHED,
    internal_selection_id: str | None = None,
    selection_process: models.DataProductProcess | None = None,
    selection_mark: bool = False,
    selection_prefix: str | None = None,
    selection_field: str = names.FLD_SELECTED,
):
    f"""
    Query rows in a collection. Index set up is the responsibility of the caller.

    storage - the storage system.
    collection - the ArangoDB collection containing the data to query.
    acceptor - a callable to accept the returned data. 
    collection_id - the ID of the KBase collection to query.
    load_ver - the load version of the KBase collection to query.
    sort_on - the field to sort on.
    sort_descending - sort in descending order rather than ascending.
    skip - the number of records to skip. Use this parameter wisely, as paging through records via
        increasing skip incrementally is an O(n^2) operation.
    start_after - skip any records prior to and including this value in the `sort_on` field,
        which should contain unique values.
        It is strongly recommended to set up an index that the query can use to skip to
        the correct starting record without a table scan. This parameter allows for
        non-O(n^2) paging of data.
    limit - the maximum number of rows to return.
    internal_match_id - an ID for a match.
    match_process - the process for a match. If provided, internal_match_id is obtained from the
        process. If the process is not complete the match information is ignored.
    match_mark - whether the match should filter or simply mark the matches.
    match_prefix - the prefix string to apply to the internal_match_id, if any.
    match_field - the name of the field in the document where the match mark should be stored.
    internal_selection_id - an ID for a selection.
    selection_process - the process for a selection. If provided, internal_selection_id is obtained
        from the process. If the process is not complete the selection information is ignored.
    selection_mark - whether the selection should filter or simply mark the selections.
    selection_prefix - the prefix string to apply to the internal_selection_id, if any.
    selection_field - the name of the field in the document where the selection mark should
        be stored.
    """
    # The number of args for these search methods is ridiculous and growing worse. A 
    # builder may be needed
    internal_match_id = _calculate_subset_id(
        internal_match_id, match_process, False, match_prefix)
    internal_selection_id = _calculate_subset_id(
        internal_selection_id, selection_process, False, selection_prefix)
    bind_vars = {
        f"@coll": collection,
        "coll_id": collection_id,
        "load_ver": load_ver,
        "sort": sort_on,
        "sort_dir": "DESC" if sort_descending else "ASC",
        "skip": skip if skip > 0 else 0,
        "limit": limit
    }
    aql = f"""
    FOR d IN @@coll
        FILTER d.{names.FLD_COLLECTION_ID} == @coll_id
        FILTER d.{names.FLD_LOAD_VERSION} == @load_ver
    """
    if start_after:
        aql += f"""
            FILTER d.@sort > @start_after
        """
        bind_vars["start_after"] = start_after
    if internal_match_id and not match_mark:
        bind_vars["internal_match_id"] = internal_match_id
        aql += f"""
            FILTER @internal_match_id IN d.{names.FLD_MATCHES_SELECTIONS}
        """
    # this will AND the match and selection. To OR, just OR the two filters instead of having
    # separate statements.
    if internal_selection_id and not selection_mark:
        bind_vars["internal_selection_id"] = internal_selection_id
        aql += f"""
            FILTER @internal_selection_id IN d.{names.FLD_MATCHES_SELECTIONS}
        """
    aql += f"""
        SORT d.@sort @sort_dir
        LIMIT @skip, @limit
        RETURN d
    """
    cur = await storage.execute_aql(aql, bind_vars=bind_vars)
    try:
        async for d in cur:
            if internal_match_id:
                d[match_field] = internal_match_id in d[names.FLD_MATCHES_SELECTIONS]
            if internal_selection_id:
                d[selection_field] = internal_selection_id in d[names.FLD_MATCHES_SELECTIONS]
            acceptor(d)
    finally:
        await cur.close(ignore_missing=True)


def _query_acceptor(
    data: list[dict[str, Any]],
    last: list[dict[str, Any]],
    doc: dict[str, Any],
    output_table: bool,
    document_mutator: Callable[[dict[str, Any]], dict[str, Any]],
):
    last[0] = doc
    if output_table:
        data.append([doc[k] for k in sorted(document_mutator(doc))])
    else:
        data.append({k: doc[k] for k in sorted(document_mutator(doc))})


class QueryTableResult(NamedTuple):
    """ The results from a query_table call. """
    skip: int
    """ The provided skip value. """
    limit: int
    """ The provided limit value. """
    fields: list[dict[str, str]] = None
    """
    The list of fields in the table, provided as "name" -> <field name> dictionaries.
    Provided if output_table is True.
    """
    table: list[list[Any]] = None
    """
    The the table data. Each column (e.g. the inner list index) in the table is described by
    the equivalent fields value,
    Provided if output_table is True.
    """
    data: list[dict[str, Any]] = None
    """
    The table data provided as a list of key / value dictionaries, which duplicates the keys
    when compared to the table view.
    Provided if output_table is False.
    """


async def query_table(
    # ew. too many args
    store: ArangoStorage,
    collection: str,
    collection_id: str,
    load_ver: str,
    sort_on: str,
    sort_descending: bool = False,
    skip: int = 0,
    limit: int = 1000,
    output_table: bool = True,
    internal_match_id: str | None = None,
    match_process: models.DataProductProcess | None = None,
    match_mark: bool = False,
    match_prefix: str | None = None,
    internal_selection_id: str | None = None,
    selection_process: models.DataProductProcess | None = None,
    selection_mark: bool = False,
    selection_prefix: str | None = None,
    document_mutator: Callable[[dict[str, Any]], dict[str, Any]] = lambda x: x,
) -> QueryTableResult:
    f"""
    Similar to query_simple_collections_list, but tailored to querying what is effectively a
    table of key / value pairs.

    If match and / or selection IDs are provided, the special keys `{names.FLD_MATCHED_SAFE}` and
    `{names.FLD_SELECTED_SAFE}` will be used to mark which rows are matched / selected by a value
    of `True`.

    storage - the storage system.
    collection - the ArangoDB collection containing the data to query.
    collection_id - the ID of the KBase collection to query.
    load_ver - the load version of the KBase collection to query.
    sort_on - the field to sort on.
    sort_descending - sort in descending order rather than ascending.
    skip - the number of records to skip. Use this parameter wisely, as paging through records via
        increasing skip incrementally is an O(n^2) operation.
    limit - the maximum number of rows to return.
    output_table - whether to return the results as a list of lists (e.g. a table) with a separate
        fields entry defining the key for each table column, or a list of key / value dictionaries.
    internal_match_id - an ID for a match.
    match_process - the process for a match. If provided, internal_match_id is obtained from the
        process. If the process is not complete the match information is ignored.
    match_mark - whether the match should filter or simply mark the matches.
    match_prefix - the prefix string to apply to the internal_match_id, if any.
    match_field - the name of the field in the document where the match mark should be stored.
    match_prefix - a prefix to apply to the match id, if supplied.
    internal_selection_id - an ID for a selection.
    selection_process - the process for a selection. If provided, internal_selection_id is obtained
        from the process. If the process is not complete the selection information is ignored.
    selection_mark - whether the selection should filter or simply mark the selections.
    selection_prefix - the prefix string to apply to the internal_selection_id, if any.
    selection_field - the name of the field in the document where the selection mark should
        be stored.
    document_mutator - a function applied to a document retrieved from the database before
        returning the results.
    """
    data = []
    last = [None]
    await query_simple_collection_list(
        store,
        collection,
        lambda doc: _query_acceptor(data, last, doc, output_table, document_mutator),
        collection_id,
        load_ver,
        sort_on,
        sort_descending=sort_descending,
        skip=skip,
        limit=limit,
        internal_match_id=internal_match_id,
        match_process=match_process,
        match_mark=match_mark,
        match_prefix=match_prefix,
        match_field=names.FLD_MATCHED_SAFE,
        internal_selection_id=internal_selection_id,
        selection_process=selection_process,
        selection_mark=selection_mark,
        selection_prefix=selection_prefix,
        selection_field=names.FLD_SELECTED_SAFE,
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
        return QueryTableResult(skip=skip, limit=limit, fields=fields, table=data)
    else:
        return QueryTableResult(skip=skip, limit=limit, data=data)


def _calculate_subset_id(
    subset_id: str | None,
    subset_process: models.DataProductProcess | None,
    subset_mark: str | None,
    subset_prefix: str | None
) -> str | None:
    # could throw an error if subset ID & process are provided at the same time... meh
    if subset_mark:
        return None
    if subset_process:
        subset_id = subset_process.internal_id if subset_process.is_complete() else None
    if subset_id and subset_prefix:
        subset_id = subset_prefix + subset_id
    return subset_id


async def count_simple_collection_list(
    storage: ArangoStorage,
    collection: str,
    collection_id: str,
    load_ver: str,
    internal_match_id: str | None = None,
    match_process: models.DataProductProcess | None = None,
    match_mark: bool = False,
    match_prefix: str | None = None,
    internal_selection_id: str | None = None,
    selection_process: models.DataProductProcess | None = None,
    selection_mark: bool = False,
    selection_prefix: str | None = None,
) -> int:
    """
    Count rows in a collection. Index set up is the responsibility of the caller.

    storage - the storage system.
    collection - the ArangoDB collection containing the data to query.
    collection_id - the ID of the KBase collection to query.
    load_ver - the load version of the KBase collection to query.
    internal_match_id - an ID for a match.
    match_process - the process for a match. If provided, internal_match_id is obtained from the
        process. If the process is not complete the match information is ignored.
    match_mark - whether the match should filter or simply mark the matches. If the latter, any 
        match information is ignored in the count.
    match_prefix - the prefix string to apply to the internal_match_id, if any.
    internal_selection_id - an ID for a selection.
    selection_process - the process for a selection. If provided, internal_selection_id is obtained
        from the process. If the process is not complete the selection information is ignored.
    selection_mark - whether the selection should filter or simply mark the selections.
        If the latter, any selection information is ignored in the count.
    selection_prefix - the prefix string to apply to the internal_selection_id, if any.
    """
    # for now this method doesn't do much. One we have some filtering implemented
    # it'll need to take that into account.

    internal_match_id = _calculate_subset_id(
        internal_match_id, match_process, match_mark, match_prefix)
    internal_selection_id = _calculate_subset_id(
        internal_selection_id, selection_process, selection_mark, selection_prefix)

    bind_vars = {
        f"@coll": collection,
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
    if internal_selection_id:
        bind_vars["internal_selection_id"] = internal_selection_id
        aql += f"""
            FILTER @internal_selection_id IN d.{names.FLD_MATCHES_SELECTIONS}
        """
    aql += f"""
        COLLECT WITH COUNT INTO length
        RETURN length
    """
    cur = await storage.execute_aql(aql, bind_vars=bind_vars)
    try:
        return await cur.next()
    finally:
        await cur.close(ignore_missing=True)


async def mark_data_by_kbase_id(
    storage: ArangoStorage,
    collection: str,
    collection_id: str,
    load_ver: str,
    kbase_ids: list[str],
    subset_internal_id: str,
) -> list[str]:
    f"""
    Mark data entries in a data product. Uses the special {names.FLD_KBASE_ID} field to find
    data entries to mark.

    It is strongly recommended to have a compound index on the fields
    `{names.FLD_COLLECTION_ID}, {names.FLD_LOAD_VERSION}, {names.FLD_KBASE_ID}`.

    The subset internal ID is added to the `{names.FLD_MATCHES_SELECTIONS}` field.

    Returns a sorted list of any IDs in the match or selection that weren't found.

    storage - the storage system.
    collection - the name of the arango collection to alter.
    collection_id - the name of the KBase collection to alter.
    load_ver - the load version of the KBase collection to alter
    kbase_ids - the ids to mark in the data set.
    subset_internal_id - the ID with with to mark the data entries in the data set, including
        any prefixes that might be necessary.
    """
    # This should be batched up, most likely. Stupid implementation for now, batch up later
    # https://stackoverflow.com/a/57877288/643675 to start and wait for multiple async routines
    selfld = names.FLD_MATCHES_SELECTIONS
    aql = f"""
        FOR d IN @@coll
            FILTER d.{names.FLD_COLLECTION_ID} == @coll_id
            FILTER d.{names.FLD_LOAD_VERSION} == @load_ver
            FILTER d.{names.FLD_KBASE_ID} IN @kbase_ids
            UPDATE d WITH {{
                {selfld}: APPEND(d.{selfld}, [@internal_id], true)
            }} IN @@coll
            OPTIONS {{exclusive: true}}
            LET updated = NEW
            RETURN KEEP(updated, "{names.FLD_KBASE_ID}")
        """
    bind_vars = {
        "@coll": collection,
        "coll_id": collection_id,
        "load_ver": load_ver,
        "kbase_ids": kbase_ids,
        "internal_id": subset_internal_id,
    }
    matched = set()
    cur = await storage.execute_aql(aql, bind_vars=bind_vars)
    try:
        async for d in cur:
            matched.add(d[names.FLD_KBASE_ID])
    finally:
        await cur.close(ignore_missing=True)
    return sorted(set(kbase_ids) - matched)


async def remove_marked_subset(
    storage: ArangoStorage,
    collection: str,
    subset_internal_id: str
):
    f"""
    Remove a set of marks from a set of data. The marks are removed from the
    `{names.FLD_MATCHES_SELECTIONS} field, and it is strongly recommended to have an index on
    that field.

    storage - the storage system.
    collection - the name of the arango collection to modify.
    subset_internal_id - the internal ID of the subset to remove, including any prefixes that
    may have been applied when marking the data.
    """
    m = names.FLD_MATCHES_SELECTIONS
    bind_vars = {
        f"@coll": collection,
        "internal_id": subset_internal_id,
    }
    aql = f"""
        FOR d IN @@coll
            FILTER @internal_id IN d.{m}
            UPDATE d WITH {{
                {m}: REMOVE_VALUE(d.{m}, @internal_id)
            }} IN @@coll
            OPTIONS {{exclusive: true}}
        """
    cur = await storage.execute_aql(aql, bind_vars=bind_vars)
    await cur.close(ignore_missing=True)
