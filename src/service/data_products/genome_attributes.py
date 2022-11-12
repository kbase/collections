"""
The genome_attribs data product, which provides geneome attributes for a collection.
"""

from fastapi import APIRouter, Request, Depends, Query
from pydantic import BaseModel, Field, Extra
import src.common.storage.collection_and_field_names as names
from src.service import app_state
from src.service import errors
from src.service.data_products.common_functions import get_load_version, remove_collection_keys
from src.service.data_products.common_models import (
    DataProductSpec,
    DBCollection,
    QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE,
)
from src.service.http_bearer import KBaseHTTPBearer, KBaseUser
from src.service.routes_common import PATH_VALIDATOR_COLLECTION_ID
from src.service.storage_arango import ArangoStorage, remove_arango_keys

# Implementation note - we known FLD_GENOME_ATTRIBS_GENOME_NAME is unique per collection id / 
# load version combination since the loader uses those 3 fields as the arango _key

ID = "genome_attribs"

_ROUTER = APIRouter(tags=["Genome Attributes"])

GENOME_ATTRIBS_SPEC = DataProductSpec(
    data_product=ID,
    router=_ROUTER,
    db_collections=[
        DBCollection(
            name=names.COLL_GENOME_ATTRIBS,
            indexes=[
                [
                    names.FLD_COLLECTION_ID,
                    names.FLD_LOAD_VERSION,
                    names.FLD_GENOME_ATTRIBS_GENOME_NAME,
                ]
            ]
        )
    ]
)


_OPT_AUTH = KBaseHTTPBearer(optional=True)


# Note these field names need to match those in src.common.storage.collection_and_field_names
class GenomeAttributes(BaseModel, extra=Extra.allow):
    """
    The set of attributes for the genome. Other than the genome name, what attributes are
    available will differ from collection to collection. Consult the loader documentation for each
    collection to determine available fields.
    """
    genome_name: str = Field(
        alias=names.FLD_GENOME_ATTRIBS_GENOME_NAME,
        example="GB_GCA_000188315.1",
        description="The genome name or ID"
    )

class AttributesForGenomes(BaseModel):
    skip: int = Field(example=0, description="The number of records that were skipped.")
    limit: int = Field(
        example=1000,
        description="The maximum number of results that could be returned."
    )
    data: list[GenomeAttributes]


_FLD_COL_ID = "colid"
_FLD_COL_NAME = "colname"
_FLD_COL_LV = "colload"
_FLD_SORT = "sort"
_FLD_SORT_DIR = "sortdir"
_FLD_SKIP = "skip"
_FLD_LIMIT = "limit"


# At some point we're going to want to filter/sort on fields. We may want a list of fields
# somewhere to check input fields are ok... but really we could just fetch the first document
# in the collection and check the fields 
@_ROUTER.get(
    f"/collections/{{collection_id}}/{ID}/",
    response_model=AttributesForGenomes,
    description="Get genome attributes for each genome in the collection, which may differ from "
        + "collection to collection.\n\n "
        + "Authentication is not required unless overriding the load version, in which case "
        + "service administration permissions are required.")
async def get_ranks(
    r: Request,
    collection_id: str = PATH_VALIDATOR_COLLECTION_ID,
    sort_on: str = Query(
        default=names.FLD_GENOME_ATTRIBS_GENOME_NAME,
        example="genome_size",
        description="The field to sort on."
    ),
    sort_desc: bool = Query(
        default=False,
        description="Whether to sort in descending order rather than ascending"
    ),
    skip: int = Query(
        default=0,
        ge=0,
        le=10000,
        example=1000,
        description="The number of records to skip"
    ),
    limit: int = Query(
        default=1000,
        ge=1,
        le=1000,
        example=1000,
        description="The maximum number of results"
    ),
    load_ver_override: str | None = QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE,
    user: KBaseUser = Depends(_OPT_AUTH)
):
    # sorting only works here since we expect the largest collection to be ~300K records and
    # we have a max limit of 1000, which means sorting is O(n log2 1000).
    # Otherwise we need indexes for every sort
    store = app_state.get_storage(r)
    load_ver = await get_load_version(store, collection_id, ID, load_ver_override, user)
    await _check_sort_field(store, collection_id, load_ver, sort_on)
    aql = f"""
    FOR d IN @@{_FLD_COL_NAME}
        FILTER d.{names.FLD_COLLECTION_ID} == @{_FLD_COL_ID}
        FILTER d.{names.FLD_LOAD_VERSION} == @{_FLD_COL_LV}
        SORT d.@{_FLD_SORT} @{_FLD_SORT_DIR}
        LIMIT @{_FLD_SKIP}, @{_FLD_LIMIT}
        RETURN d
    """
    bind_vars = {
        f"@{_FLD_COL_NAME}": names.COLL_GENOME_ATTRIBS,
        _FLD_COL_ID: collection_id,
        _FLD_COL_LV: load_ver,
        _FLD_SORT: sort_on,
        _FLD_SORT_DIR: "DESC" if sort_desc else "ASC",
        _FLD_SKIP: skip,
        _FLD_LIMIT: limit
    }
    # could get the doc count, but that'll be slower since all docs have to be counted vs. just
    # getting LIMIT docs. YAGNI for now
    cur = await store.aql().execute(aql, bind_vars=bind_vars)
    ret = []
    async for d in cur:
        # not clear there's any benefit to creating a bunch of TaxaCount objects here
        ret.append(remove_collection_keys(remove_arango_keys(d)))
    return {_FLD_SKIP: skip, _FLD_LIMIT: limit, "data": ret}


async def _check_sort_field(store: ArangoStorage, collection_id: str, load_ver: str, field: str):
    if field == names.FLD_GENOME_ATTRIBS_GENOME_NAME:
        return
    aql = f"""
    FOR d IN @@{_FLD_COL_NAME}
        FILTER d.{names.FLD_COLLECTION_ID} == @{_FLD_COL_ID}
        FILTER d.{names.FLD_LOAD_VERSION} == @{_FLD_COL_LV}
        LIMIT 1
        RETURN d
    """
    bind_vars = {
        f"@{_FLD_COL_NAME}": names.COLL_GENOME_ATTRIBS,
        _FLD_COL_ID: collection_id,
        _FLD_COL_LV: load_ver,
    }
    cur = await store.aql().execute(aql, bind_vars=bind_vars)
    doc = await cur.next()
    if not doc:
        return  # no data, so no sorting.
    if field not in doc:
        raise errors.IllegalParameterError(
            f"No such field for collection {collection_id} load version {load_ver}: {field}")
    