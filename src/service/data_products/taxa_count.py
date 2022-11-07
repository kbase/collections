"""
The taxa_count data product, which provides taxa counts for a collection at each taxonomy rank.
"""

from fastapi import APIRouter, Request, Depends
from pydantic import BaseModel, Field
from src.common.hash import md5_string
from src.service import app_state
from src.service import errors
from src.service.data_products.common_functions import get_load_version
from src.service.data_products.common_models import (
    DataProductSpec,
    DBCollection,
    QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE,
)
from src.service.http_bearer import KBaseHTTPBearer, KBaseUser
import src.common.storage.collection_and_field_names as names
from src.service.routes_common import PATH_VALIDATOR_COLLECTION_ID


ID = "taxa_count"

_ROUTER = APIRouter(tags=["Taxa count"])

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
                    names.FLD_COLLECTION_NAME,
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


class Ranks(BaseModel):
    data: list[str] = Field(
        example=["Domain", "Phylum"],
        description="A list of taxonomy ranks in rank order"
    )


_FLD_COL_ID = "colid"
_FLD_KEY = "key"


@_ROUTER.get(
    f"/collections/{{collection_id}}/{ID}/ranks",
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
    aql = f"""
        FOR d IN @@{_FLD_COL_ID}
            FILTER d.{names.FLD_ARANGO_KEY} == @{_FLD_KEY}
            return d
    """
    bind_vars = {
        f"@{_FLD_COL_ID}": names.COLL_TAXA_COUNT_RANKS,
        _FLD_KEY: ranks_key(collection_id, load_ver)
    }
    cur = await store.aql().execute(aql, bind_vars=bind_vars, count=True)
    if cur.count() < 1:
        err = f"No data loaded for {collection_id} collection load version {load_ver}"
        if load_ver_override:
            raise errors.NoDataFoundError(err)
        raise ValueError(err)
    # since we're getting a doc by _key > 1 is impossible
    doc = await cur.next()
    return Ranks(data=doc[names.FLD_TAXA_COUNT_RANKS])


# TODO DATAPROD add counts endpoint
