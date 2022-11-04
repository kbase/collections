"""
Data structures common to all data products
"""

from fastapi import APIRouter, Query
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


QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE = Query(
    default=None,
    min_length=models.LENGTH_MIN_LOAD_VERSION,
    max_length=models.LENGTH_MAX_LOAD_VERSION,
    regex=models.REGEX_LOAD_VERSION,
    example=models.FIELD_LOAD_VERSION_EXAMPLE,
    description=models.FIELD_LOAD_VERSION_DESCRIPTION + ". This will override the collection's "
        + "load version. Service administrator privileges are required."
)
