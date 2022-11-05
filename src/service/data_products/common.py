"""
Data structures and methods common to all data products
"""

from fastapi import APIRouter
from pydantic import BaseModel, validator
from src.service import models
from src.service import errors

class DBCollection(BaseModel):
    """
    Defines a database collection that a data product uses and required indexes for that
    collection.
    """
    
    name: str
    """ The collection name in the database. """

    indexes: list[list[str]]
    """
    The indexes in the collection. Each item in the outer list is an index, with the inner
    list defining the fields of the (potentially coumpound) index. For example:

    [
        # indexes for taxa_count rank data
        ["coll", "load_ver", "rank", "count"],
        # indexes for taxa_count rank lists
        ["coll", "load_ver"]
    ]
    """


class DataProductSpec(BaseModel):
    """
    A specification that defines the parts and requirements of a data product.
    """

    data_product: str
    """
    The ID of the data product. This ID is used in several places:
    
    * The URI paths for the data product should start with
        /collections/{collection_id}/<data_product>/...
    * In the data_products section of a Collection document, including a data product object
        with this ID indicates that the Collection has this data product activated
    * Optionally, but recommended, in the database collection names.
    """

    db_collections: list[DBCollection]
    """ The database collections required for the data product. """

    router: APIRouter
    """
    The router for the Collection endpoints. The router MUST be created with at least one entry
    in the `tags` argument.
    """

    @validator("router")
    def _check_router_tags(cls, v):
        if not v.tags:
            raise ValueError("router must have at least one tag")
        return v

    class Config:
        arbitrary_types_allowed = True


def get_load_version(ac: models.ActiveCollection, data_product: str):
    """
    Get the load version of a data product given a Collection and the ID of the data product,
    or throw an error if the Collection does not have the data product activated.
    """
    for dp in ac.data_products:
        if dp.product == data_product:
            return dp.version
    raise errors.NoRegisteredDataProduct(
        f"The {ac.id} collection does not have a {data_product} data product registered.")
