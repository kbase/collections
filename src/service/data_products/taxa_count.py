"""
The taxa_count data product, which provides taxa counts for a collection at each taxonomy rank.
"""

from fastapi import APIRouter, Request, Depends, Path
from pydantic import BaseModel, Field
from src.common.hash import md5_string
import src.common.storage.collection_and_field_names as names
from src.service import app_state
from src.service import errors
from src.service.data_products.common_functions import get_load_version
from src.service.data_products.common_models import (
    DataProductSpec,
    DBCollection,
    QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE,
)
from src.service.http_bearer import KBaseHTTPBearer, KBaseUser
from src.service.routes_common import PATH_VALIDATOR_COLLECTION_ID
from src.service.storage_arango import ArangoStorage, remove_arango_keys


ID = "taxa_count"

_ROUTER = APIRouter(tags=["Taxa count"], prefix=f"/{ID}")

TAXA_COUNT_SPEC = DataProductSpec(
    data_product=ID,
    router=_ROUTER,
    db_collections=[
        DBCollection(
            name=names.COLL_TAXA_COUNT_RANKS,
            indexes=[]
        ),
        DBCollection(
            name=names.COLL_TAXA_COUNT,
            indexes=[
                [
                    names.FLD_COLLECTION_ID,
                    names.FLD_LOAD_VERSION,
                    names.FLD_TAXA_COUNT_RANK,
                    names.FLD_TAXA_COUNT_COUNT
                ]
            ]
        )
    ]
)

_OPT_AUTH = KBaseHTTPBearer(optional=True)


def ranks_key(collection_id: str, load_ver: str):
    f"""
    Calculate the ranks database key for the {ID} data product.
    """
    return md5_string(f"{collection_id}_{load_ver}")


# modifies in place
def _remove_counts_keys(doc: dict):
    for k in [names.FLD_TAXA_COUNT_RANK, names.FLD_COLLECTION_ID, names.FLD_LOAD_VERSION]:
        doc.pop(k, None)
    return doc


class Ranks(BaseModel):
    data: list[str] = Field(
        example=["domain", "phylum"],
        description="A list of taxonomy ranks in rank order"
    )


# Note these field names need to match those in src.common.storage.collection_and_field_names
class TaxaCount(BaseModel):
    name: str = Field(
        example="Marininema halotolerans",
        description="The name of the taxa"
    )
    count: int = Field(
        example=42,
        description="The number of genomes in the collection in this taxa"
    )


class TaxaCounts(BaseModel):
    data: list[TaxaCount]


_FLD_COL_ID = "colid"
_FLD_KEY = "key"
_FLD_COL_NAME = "colname"
_FLD_COL_LV = "colload"
_FLD_COL_RANK = "rank"


@_ROUTER.get(
    "/ranks/",
    response_model=Ranks,
    description="Get the taxonomy ranks, in rank order, for the taxa counts.\n\n "
        + "Authentication is not required unless overriding the load version, in which case "
        + "service administration permissions are required.")
async def get_ranks(
    r: Request,
    collection_id: str = PATH_VALIDATOR_COLLECTION_ID,
    load_ver_override: str | None = QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE,
    user: KBaseUser = Depends(_OPT_AUTH)
):
    store = app_state.get_storage(r)
    load_ver = await get_load_version(store, collection_id, ID, load_ver_override, user)
    return await get_ranks_from_db(store, collection_id, load_ver, bool(load_ver_override))


async def get_ranks_from_db(
    store: ArangoStorage,
    collection_id: str,
    load_ver: str,
    load_ver_overridden: bool,
):
    aql = f"""
        FOR d IN @@{_FLD_COL_ID}
            FILTER d.{names.FLD_ARANGO_KEY} == @{_FLD_KEY}
            RETURN d
    """
    bind_vars = {
        f"@{_FLD_COL_ID}": names.COLL_TAXA_COUNT_RANKS,
        _FLD_KEY: ranks_key(collection_id, load_ver)
    }
    cur = await store.aql().execute(aql, bind_vars=bind_vars, count=True)
    if cur.count() < 1:
        err = f"No data loaded for {collection_id} collection load version {load_ver}"
        if load_ver_overridden:
            raise errors.NoDataFoundError(err)
        raise ValueError(err)
    # since we're getting a doc by _key > 1 is impossible
    doc = await cur.next()
    return Ranks(data=doc[names.FLD_TAXA_COUNT_RANKS])


@_ROUTER.get(
    "/counts/{rank}/",
    response_model=TaxaCounts,
    description="Get the taxonomy counts in descending order. At most 20 taxa are returned.\n\n "
        + "Authentication is not required unless overriding the load version, in which case "
        + "service administration permissions are required.")
async def get_taxa_counts(
    r: Request,
    collection_id: str = PATH_VALIDATOR_COLLECTION_ID,
    rank: str = Path(
        example="phylum",
        description="The taxonomic rank at which to return results"
    ),
    load_ver_override: str | None = QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE,
    user: KBaseUser = Depends(_OPT_AUTH)
):
    store = app_state.get_storage(r)
    load_ver = await get_load_version(store, collection_id, ID, load_ver_override, user)
    ranks = await get_ranks_from_db(store, collection_id, load_ver, bool(load_ver_override))
    if rank not in ranks.data:
        raise errors.IllegalParameterError(f"Invalid rank: {rank}")
    aql = f"""
        FOR d IN @@{_FLD_COL_NAME}
            FILTER d.{names.FLD_COLLECTION_ID} == @{_FLD_COL_ID}
            FILTER d.{names.FLD_LOAD_VERSION} == @{_FLD_COL_LV}
            FILTER d.{names.FLD_TAXA_COUNT_RANK} == @{_FLD_COL_RANK}
            SORT d.{names.FLD_TAXA_COUNT_COUNT} DESC
            LIMIT 20
            RETURN d
    """
    # will probably want some sort of sort / limit options, but don't get too crazy. Wait for
    # feedback for now
    bind_vars = {
        f"@{_FLD_COL_NAME}": names.COLL_TAXA_COUNT,
        _FLD_COL_ID: collection_id,
        _FLD_COL_LV: load_ver,
        _FLD_COL_RANK: rank
    }
    # could get the doc count, but that'll be slower since all docs have to be counted vs. just
    # getting LIMIT docs. YAGNI for now
    cur = await store.aql().execute(aql, bind_vars=bind_vars)
    ret = []
    async for d in cur:
        # not clear there's any benefit to creating a bunch of TaxaCount objects here
        ret.append(_remove_counts_keys(remove_arango_keys(d)))
    return {"data": ret}
