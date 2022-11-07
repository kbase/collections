"""
Functions common to all data products
"""

from src.service import errors
from src.service import kb_auth
from src.service.storage_arango import ArangoStorage
from src.service.http_bearer import KBaseUser



async def get_load_version(
    store: ArangoStorage,
    collection_id: str,
    data_product: str,
    load_ver: str,
    user: KBaseUser,
):
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
    for dp in ac.data_products:
        if dp.product == data_product:
            return dp.version
    raise errors.NoRegisteredDataProduct(
        f"The {ac.id} collection does not have a {data_product} data product registered.")
