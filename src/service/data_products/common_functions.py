"""
Functions common to all data products
"""

import src.common.storage.collection_and_field_names as names
from src.common.storage.db_doc_conversions import collection_load_version_key
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
    exists per collection load version.
    
    store - the storage system.
    collection - the arango collection containing the document.
    collection_id - the KBase collection containing the document.
    load_ver - the load version of the collection.
    no_data_error - raise a NoDataFoundError (indicating a caller error) instead of a ValueError
        (indicating a problem with the database) if the document isn't found.

    """
    aql = f"""
        FOR d IN @@coll
            FILTER d.{names.FLD_ARANGO_KEY} == @key
            RETURN d
    """
    bind_vars = {
        "@coll": collection,
        "key": collection_load_version_key(collection_id, load_ver),
    }
    cur = await store.aql().execute(aql, bind_vars=bind_vars, count=True)
    try:
        if cur.count() < 1:
            err = f"No data loaded for {collection_id} collection load version {load_ver}"
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
    limit: int = 1000,
    internal_match_id: str | None = None,
    match_mark: bool = False,
    internal_selection_id: str | None = None,
    selection_mark: bool = False,
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
    limit - the maximum number of rows to return.
    internal_match_id - an ID for a match.
    match_mark - if True, don't filter based on the match, just mark matched rows.
    internal_selection_id - an ID for a selection.
    selection_mark - if True, don't filter based on the selection, just mark selected rows.

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
                d[names.FLD_MATCHED] = internal_match_id in d[names.FLD_MATCHES_SELECTIONS]
            if internal_selection_id:
                d[names.FLD_SELECTED] = internal_selection_id in d[names.FLD_MATCHES_SELECTIONS]
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
