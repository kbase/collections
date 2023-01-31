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
from typing import Any

# Implementation note - we know FLD_GENOME_ATTRIBS_KBASE_GENOME_ID is unique per collection id /
# load version combination since the loader uses those 3 fields as the arango _key

ID = "genome_attribs"

_ROUTER = APIRouter(tags=["Genome Attributes"], prefix=f"/{ID}")

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
                    names.FLD_GENOME_ATTRIBS_KBASE_GENOME_ID,
                    # Since this is the default sort option (see below), we specify an index
                    # for fast sorts since every time the user hits the UI for the first time
                    # or without specifying a sort order it'll sort on this field
                ]
            ]
        )
    ]
)


_OPT_AUTH = KBaseHTTPBearer(optional=True)

def _remove_keys(doc):
    doc = remove_collection_keys(remove_arango_keys(doc))
    doc.pop(names.FLD_GENOME_ATTRIBS_MATCHES, None)
    return doc


class AttributeName(BaseModel):
    name: str = Field(
        example=names.FLD_GENOME_ATTRIBS_KBASE_GENOME_ID,
        description="The name of an attribute"
    )

class GenomeAttributes(BaseModel, extra=Extra.allow):
    """
    Attributes for a set of genomes. Either `fields` and `table` are returned, `data` is
    returned, or `count` is returned.
    The set of available attributes may be different for different collections.
    """
    skip: int = Field(example=0, description="The number of records that were skipped.")
    limit: int = Field(
        example=1000,
        description="The maximum number of results that could be returned. "
            + "0 and meaningless if `count` is specified"
    )
    # may need to return fields with data in the future if we add more info to fields
    fields: list[AttributeName] | None = Field(
        description="The name for each column in the attribute table."
    )
    table: list[list[Any]] | None = Field(
        example=[["my_genome_name"]],
        description="The attributes in an NxM table. Each column's name is available at the "
            + "corresponding index in the fields parameter. Each inner list is a row in the "
            + "table with each entry being the entry for that column."
    )
    data: list[dict[str, Any]] | None = Field(
        example=[{names.FLD_GENOME_ATTRIBS_KBASE_GENOME_ID: "assigned_kbase_genome_id"}],
        description="The attributes as a list of dictionaries."
    )
    count: int | None = Field(
        example=42,
        description="The number of attribute records that match the query.",
    )

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
    "/",
    response_model=GenomeAttributes,
    description="Get genome attributes for each genome in the collection, which may differ from "
        + "collection to collection.\n\n "
        + "Authentication is not required unless overriding the load version, in which case "
        + "service administration permissions are required.")
async def get_genome_attributes(
    r: Request,
    collection_id: str = PATH_VALIDATOR_COLLECTION_ID,
    sort_on: str = Query(
        default=names.FLD_GENOME_ATTRIBS_KBASE_GENOME_ID,
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
    output_table: bool = Query(
        default=True,
        description="Whether to return the data in table form or dictionary list form"
    ),
    count: bool = Query(
        default = False,
        description="Whether to return the number of records that match the query rather than "
            + "the records themselves"
    ),
    load_ver_override: str | None = QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE,
    user: KBaseUser = Depends(_OPT_AUTH)
):
    # sorting only works here since we expect the largest collection to be ~300K records and
    # we have a max limit of 1000, which means sorting is O(n log2 1000).
    # Otherwise we need indexes for every sort
    store = app_state.get_storage(r)
    load_ver = await get_load_version(store, collection_id, ID, load_ver_override, user)
    if count:
        return await _count(store, collection_id, load_ver)
    else:
        return await _query(
            store, collection_id, load_ver, sort_on, sort_desc, skip, limit, output_table) 


async def _query(
    # ew. too many args
    store: ArangoStorage,
    collection_id: str,
    load_ver: str,
    sort_on: str,
    sort_desc: bool,
    skip: int,
    limit: int,
    output_table: bool,
):
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
    # Sort everything since we can't necessarily rely on arango, the client, or the loader
    # to have the same insertion order for the dicts
    # If we want a specific order the loader should stick a keys doc or something into arango
    # and we order by that
    data = []
    d = None
    async for d in cur:
        if output_table:
            data.append([d[k] for k in sorted(_remove_keys(d))])
        else:
            data.append({k: d[k] for k in sorted(_remove_keys(d))})
    fields = []
    if d:
        if sort_on not in d: 
            raise errors.IllegalParameterError(
                f"No such field for collection {collection_id} load version {load_ver}: {sort_on}")
        fields = [{"name": k} for k in sorted(d)]
    if output_table:
        return {_FLD_SKIP: skip, _FLD_LIMIT: limit, "fields": fields, "table": data}
    else:
        return {_FLD_SKIP: skip, _FLD_LIMIT: limit, "data": data}


async def _count(store: ArangoStorage, collection_id: str, load_ver: str):
    # for now this method doesn't do much. One we have some matching and filtering implemented
    # it'll need to take that into account.

    aql = f"""
    FOR d IN @@{_FLD_COL_NAME}
        FILTER d.{names.FLD_COLLECTION_ID} == @{_FLD_COL_ID}
        FILTER d.{names.FLD_LOAD_VERSION} == @{_FLD_COL_LV}
        COLLECT WITH COUNT INTO length
        RETURN length
    """
    bind_vars = {
        # will be different if matches are stored in a different collection
        f"@{_FLD_COL_NAME}": names.COLL_GENOME_ATTRIBS,
        _FLD_COL_ID: collection_id,
        _FLD_COL_LV: load_ver,
    }
    cur = await store.aql().execute(aql, bind_vars=bind_vars)
    return {_FLD_SKIP: 0, _FLD_LIMIT: 0, "count": await cur.next()}
