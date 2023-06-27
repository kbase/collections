"""
Data structures common to all data products
"""

from fastapi import APIRouter, Query
from pydantic import BaseModel, validator, Field
from src.common.storage import collection_and_field_names as names
from src.service import models
from src.service import errors
from typing import Annotated


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

    data_product: str = models.DATA_PRODUCT_ID_FIELD
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


QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE = Annotated[str | None, Query(
    min_length=models.LENGTH_MIN_LOAD_VERSION,
    max_length=models.LENGTH_MAX_LOAD_VERSION,
    regex=models.REGEX_LOAD_VERSION,
    example=models.FIELD_LOAD_VERSION_EXAMPLE,
    description=models.FIELD_LOAD_VERSION_DESCRIPTION + ". This will override the collection's "
        + "load version. Service administrator privileges are required."
)]


QUERY_VALIDATOR_SKIP = Annotated[int, Query(
    ge=0,
    le=10000,
    example=1000,
    description="The number of records to skip"
)]


QUERY_VALIDATOR_LIMIT = Annotated[int, Query(
    ge=1,
    le=1000,
    example=1000,
    description="The maximum number of results"
)]


QUERY_VALIDATOR_COUNT = Annotated[bool, Query(
    description="Whether to return the number of records that match the query rather than "
        + "the records themselves. Paging parameters are ignored."
)]


QUERY_VALIDATOR_MATCH_ID = Annotated[str | None, Query(
    description="A match ID to set the view to the match rather than "
        + "the entire collection. Authentication is required. If a match ID is "
        # matches are against a specific load version, so...
        + "set, any load version override is ignored. "
        + "If a selection filter and a match filter are provided, they are ANDed together. "
        + "Has no effect on a `count` if `match_mark` is true."
)]


QUERY_VALIDATOR_MATCH_MARK = Annotated[bool, Query(
    description="Whether to mark matched rows rather than filter based on the match ID."
)]


QUERY_VALIDATOR_MATCH_MARK_SAFE = Annotated[bool, Query(
    description="Whether to mark matched rows rather than filter based on the match ID. "
        + "Matched rows will be indicated by a true value in the special field "
        + f"`{names.FLD_MATCHED_SAFE}`."
)]


QUERY_VALIDATOR_SELECTION_ID = Annotated[str | None, Query(
    description="A selection ID to set the view to the selection rather than the entire "
        + "collection. If a selection ID is set, any load version override is ignored. "
        + "If a selection filter and a match filter are provided, they are ANDed together. "
        + "Has no effect on a `count` if `selection_mark` is true."
)]


QUERY_VALIDATOR_SELECTION_MARK = Annotated[bool, Query(
    description="Whether to mark selected rows rather than filter based on the selection ID."
)]


QUERY_VALIDATOR_SELECTION_MARK_SAFE = Annotated[bool, Query(
    description="Whether to mark selected rows rather than filter based on the selection ID. "
            + "Selected rows will be indicated by a true value in the special field "
            + f"`{names.FLD_SELECTED_SAFE}`."
)]


QUERY_VALIDATOR_STATUS_ONLY = Annotated[bool, Query(
    description="Only return the status of any match or selection processing without any data."
)]


QUERY_VALIDATOR_SORT_ON = Annotated[str | None, Query(
    example=names.FLD_KBASE_ID,
    description="The field to sort on."
)]


QUERY_VALIDATOR_SORT_DIRECTION = Annotated[bool, Query(
    description="Whether to sort in descending order rather than ascending"
)]


QUERY_VALIDATOR_OUTPUT_TABLE = Annotated[bool, Query(
    description="Whether to return the data in table form or dictionary list form"
)]