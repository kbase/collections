"""
Functions common to all data products
"""
from typing import Any, Callable, NamedTuple

from fastapi import Request

import src.common.storage.collection_and_field_names as names
from src.common.product_models import columnar_attribs_common_models as col_models
from src.common.storage.db_doc_conversions import (
    collection_load_version_key,
    collection_data_id_key,
)
from src.service import errors, kb_auth, models, app_state
from src.service.filtering.filters import FilterSet
from src.service.storage_arango import ArangoStorage


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


COLLECTION_KEYS = {names.FLD_COLLECTION_ID, names.FLD_LOAD_VERSION}
"""
Special keys in data sets that denote the ID and load version of a collection.
Usually not returned to the user but needed in the database.
"""

def remove_collection_keys(doc: dict):
    """ Removes the collection ID and load version keys from a dictionary **in place**. """
    for k in COLLECTION_KEYS:
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


async def get_columnar_attribs_meta(
        r: Request,
        collection: str,
        collection_id: str,
        data_product: str,
        load_ver_override,
        user: kb_auth.KBaseUser,
        return_only_visible: bool = False

) -> col_models.ColumnarAttributesMeta:
    """
    Get the columnar attributes meta document for a collection. The document is expected to be
    a singleton per collection load version.

    r - the request.
    collection - the arango collection containing the document.
    collection_id - the ID of the Collection from which to retrieve the load version and possibly
        collection object.
    data_product - the ID of the data product from which to retrieve the load version.
    load_ver_override - an override for the load version. If provided:
        * the user must be a service administrator
        * the collection is not checked for the existence of the data product.
    user - the user. Ignored if load_ver is not provided; must be a service administrator.
    return_only_visible - whether to return only visible columns. Default false.

    """
    storage = app_state.get_app_state(r).arangostorage
    _, load_ver = await get_load_version(storage, collection_id, data_product, load_ver_override, user)

    doc = await get_collection_singleton_from_db(
            storage,
            collection,
            collection_id,
            load_ver,
            bool(load_ver_override)
    )
    doc[col_models.FIELD_COLUMNS] = [col_models.AttributesColumn(**d)
                                     for d in doc[col_models.FIELD_COLUMNS]]

    meta = col_models.ColumnarAttributesMeta(**remove_collection_keys(doc))
    if return_only_visible:
        meta.columns = [c for c in meta.columns if not c.non_visible]

    return meta


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
    storage: ArangoStorage,
    filters: FilterSet,
    acceptor: Callable[[dict[str, Any]], None],
    match_field: str = names.FLD_MATCHED,
    selection_field: str = names.FLD_SELECTED,
):
    f"""
    Query rows in a collection. Index set up is the responsibility of the caller.

    storage - the storage system.
    filters - the filters to apply to the search.
    acceptor - a callable to accept the returned data. If filters.count is true, the count will
        be returned to the acceptor.
    match_field - the name of the field in the document where the match mark should be stored.
    selection_field - the name of the field in the document where the selection mark should
        be stored.
    """
    aql, bind_vars = filters.to_aql()
    cur = await storage.execute_aql(aql, bind_vars=bind_vars)
    try:
        async for d in cur:
            if not filters.count:
                if not filters.match_spec.is_null_subset():
                    d[match_field] = filters.match_spec.get_prefixed_subset_id() in d[
                        names.FLD_MATCHES_SELECTIONS]
                if not filters.selection_spec.is_null_subset():
                    d[selection_field] = filters.selection_spec.get_prefixed_subset_id() in d[
                        names.FLD_MATCHES_SELECTIONS]
            acceptor(d)
    finally:
        await cur.close(ignore_missing=True)


def _query_acceptor(
    data: list[dict[str, Any]],
    last: list[dict[str, Any]],
    doc: dict[str, Any],
    output_table: bool,
    document_mutator: Callable[[dict[str, Any]], dict[str, Any]],
    count: bool,
):
    last[0] = doc
    if count:
        data.append(doc)
    elif output_table:
        data.append([doc[k] for k in sorted(document_mutator(doc))])
    else:
        data.append({k: doc[k] for k in sorted(document_mutator(doc))})


class QueryTableResult(NamedTuple):
    """ The results from a query_table call. """
    skip: int
    """ The provided skip value. """
    limit: int
    """ The provided limit value. """
    count: int = None
    """ The count of the results. If provided, fields, table, and data will be null. """
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
    store: ArangoStorage,
    filters: FilterSet,
    output_table: bool = True,
    document_mutator: Callable[[dict[str, Any]], dict[str, Any]] = lambda x: x,
) -> QueryTableResult:
    f"""
    Similar to query_simple_collections_list, but tailored to querying what is effectively a
    table of key / value pairs.

    If match and / or selection IDs are provided in the filter set,
    the special keys `{names.FLD_MATCHED_SAFE}` and `{names.FLD_SELECTED_SAFE}`
    will be used to mark which rows are matched / selected by a value of `True`.

    storage - the storage system.
    filters - the filters to apply to the search
    output_table - whether to return the results as a list of lists (e.g. a table) with a separate
        fields entry defining the key for each table column, or a list of key / value dictionaries.
    document_mutator - a function applied to a document retrieved from the database before
        returning the results.
    """
    data = []
    last = [None]
    await query_simple_collection_list(
        store,
        filters,
        lambda doc: _query_acceptor(
            data, last, doc, output_table, document_mutator, filters.count),
        match_field=names.FLD_MATCHED_SAFE,
        selection_field=names.FLD_SELECTED_SAFE,
    )
    if filters.count:
        return QueryTableResult(skip=0, limit=0, count=data[0])
    # Sort everything since we can't necessarily rely on arango, the client, or the loader
    # to have the same insertion order for the dicts
    # If we want a specific order the loader should stick a keys doc or something into arango
    # and we order by that
    fields = []
    if last[0]:
        if filters.sort_on not in last[0]: 
            raise errors.IllegalParameterError(
                f"No such field for collection {filters.collection_id} load version "
                + f"{filters.load_ver}: {filters.sort_on}")
        fields = [{"name": k} for k in sorted(last[0])]
    if output_table:
        return QueryTableResult(skip=filters.skip, limit=filters.limit, fields=fields, table=data)
    else:
        return QueryTableResult(skip=filters.skip, limit=filters.limit, data=data)


async def mark_data_by_kbase_id(
    storage: ArangoStorage,
    collection: str,
    collection_id: str,
    load_ver: str,
    kbase_ids: list[str],
    subset_internal_id: str,
    multiple_ids: bool = False,
) -> list[str]:
    f"""
    Mark data entries in a data product. Uses the special {names.FLD_KBASE_ID} or
    {names.FLD_KBASE_IDS} fields to find data entries to mark.

    It is strongly recommended to have a compound index on the fields
    `{names.FLD_COLLECTION_ID}, {names.FLD_LOAD_VERSION}, {names.FLD_KBASE_ID} /
    {names.FLD_KBASE_IDS}`.

    The subset internal ID is added to the `{names.FLD_MATCHES_SELECTIONS}` field.

    Returns a sorted list of any IDs in the match or selection that weren't found.

    storage - the storage system.
    collection - the name of the arango collection to alter.
    collection_id - the name of the KBase collection to alter.
    load_ver - the load version of the KBase collection to alter
    kbase_ids - the ids to mark in the data set.
    subset_internal_id - the ID with with to mark the data entries in the data set, including
        any prefixes that might be necessary.
    multiple_ids - queries against the {names.FLD_KBASE_IDS} field and expects to find a list
        of ids in that field if True.
    """
    # This should be batched up, most likely. Stupid implementation for now, batch up later
    # https://stackoverflow.com/a/57877288/643675 to start and wait for multiple async routines
    selfld = names.FLD_MATCHES_SELECTIONS
    idfield = names.FLD_KBASE_IDS if multiple_ids else names.FLD_KBASE_ID
    bind_vars = {
        "@coll": collection,
        "coll_id": collection_id,
        "load_ver": load_ver,
        "internal_id": subset_internal_id,
        "retfield": idfield,
    }
    aql = f"""
        FOR d IN @@coll
            FILTER d.{names.FLD_COLLECTION_ID} == @coll_id
            FILTER d.{names.FLD_LOAD_VERSION} == @load_ver"""
    if not multiple_ids:
        aql += f"""
            FILTER d.{idfield} IN @kbase_ids"""
        bind_vars["kbase_ids"] = kbase_ids
    else:
        lines = []
        for i, kbid in enumerate(kbase_ids):  # ANY IN doesn't use indexes, so it's this mess
            lines.append(f"@kbase_id{i} IN d.{idfield}")
            bind_vars[f"kbase_id{i}"] = kbid
        aql += f"""
            FILTER
                """ + "\n                ||\n            ".join(lines)
    aql += f"""
            UPDATE d WITH {{
                {selfld}: APPEND(d.{selfld}, [@internal_id], true)
            }} IN @@coll
            OPTIONS {{exclusive: true}}
            LET updated = NEW
            RETURN KEEP(updated, @retfield)
        """
    matched = set()
    cur = await storage.execute_aql(aql, bind_vars=bind_vars)
    try:
        async for d in cur:
            matched.update(d[idfield]) if multiple_ids else matched.add(d[idfield])
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
