"""
Functions common to all data products
"""

import src.common.storage.collection_and_field_names as names
from src.common.storage.db_doc_conversions import collection_load_version_key
from src.service import errors
from src.service import models
from src.service import kb_auth
from src.service.storage_arango import ArangoStorage

from typing import Any


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
    Get a document from the database where it is expected that only once instance of that document
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
