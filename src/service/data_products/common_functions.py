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

from typing import Any, Callable


async def get_load_version(
    store: ArangoStorage,
    collection_id: str,
    data_product: str,
    load_ver: str,
    user: kb_auth.KBaseUser,
) -> str:
    """
    Get the load version of a data product given a Collection ID and the ID of the data product,
    optionally allowing an override of the load versionn if the user is a service administrator.

    store - the data storage system.
    collection_id - the ID of the Collection from which to retrive the load version.
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
        return load_ver
    ac = await store.get_collection_active(collection_id)
    return get_load_ver_from_collection(ac, data_product)


def get_load_ver_from_collection(collection: models.SavedCollection, data_product: str) -> str:
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
    cur = await store.aql().execute(aql, bind_vars=bind_vars, count=True)
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
    match_mark: bool = False,
    match_field: str = names.FLD_MATCHED,
    internal_selection_id: str | None = None,
    selection_mark: bool = False,
    selection_field: str = names.FLD_SELECTED
):
    f"""
    Query rows in a collection. Index set up is the responsibilty of the caller.

    If match and / or selection IDs are provided, the special keys `{names.FLD_MATCHED}` and
    `{names.FLD_SELECTED}` will be used to mark which rows are matched / selected by a value
    of `True`.

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
    match_mark - if True, don't filter based on the match, just mark matched rows.
    match_field - the name of the field in the document where the match mark should be stored.
    internal_selection_id - an ID for a selection.
    selection_mark - if True, don't filter based on the selection, just mark selected rows.
    selection_field - the name of the field in the document where the selection mark should
        be stored.
    """
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
    cur = await storage.aql().execute(aql, bind_vars=bind_vars)
    try:
        async for d in cur:
            if internal_match_id:
                d[match_field] = internal_match_id in d[names.FLD_MATCHES_SELECTIONS]
            if internal_selection_id:
                d[selection_field] = internal_selection_id in d[names.FLD_MATCHES_SELECTIONS]
            acceptor(d)
    finally:
        await cur.close(ignore_missing=True)


async def count_simple_collection_list(
    storage: ArangoStorage,
    collection: str,
    collection_id: str,
    load_ver: str,
    internal_match_id: str | None,
    internal_selection_id: str | None,
) -> int:
    """
    Count rows in a collection. Index set up is the responsibilty of the caller.

    storage - the storage system.
    collection - the ArangoDB collection containing the data to query.
    collection_id - the ID of the KBase collection to query.
    load_ver - the load version of the KBase collection to query.
    internal_match_id - an ID for a match.
    internal_selection_id - an ID for a selection.
    """
    # for now this method doesn't do much. One we have some filtering implemented
    # it'll need to take that into account.

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
    cur = await storage.aql().execute(aql, bind_vars=bind_vars)
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
        any prefixes that might be necessasry.
    """
    # This should be batched up, most likely. Stupid implmentation for now, batch up later
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
    # TODO TEST Should probably change this to storage.execute_aql(aql, bind_vars={}, count=False)
    #           Cleaner, less internals exposed, easier to mock for tests
    cur = await storage.aql().execute(aql, bind_vars=bind_vars)
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
    cur = await storage.aql().execute(aql, bind_vars=bind_vars)
    await cur.close(ignore_missing=True)
