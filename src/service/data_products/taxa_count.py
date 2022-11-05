"""
The taxa_count data product, which provides taxa counts for a collection at each taxonomy rank.
"""

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field
from src.service import app_state
from src.service.data_products.common import DataProductSpec, DBCollection, get_load_version
import src.common.storage.collection_and_field_names as names
from src.service.routes_common import PATH_VALIDATOR_COLLECTION_ID
from src.common.hash import md5_string


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


# TODO DATAPROD add auth and allow admins to override load_ver
@_ROUTER.get(f"/collections/{{collection_id}}/{ID}/ranks", response_model=Ranks)
async def get_ranks(r: Request, collection_id: str = PATH_VALIDATOR_COLLECTION_ID):
    store = app_state.get_storage(r)
    ac = await store.get_collection_active(collection_id)
    load_ver = get_load_version(ac, ID)
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
        # if an admin overrides load_ver this should be a 400 error, not 500
        raise ValueError(f"No data loaded for {collection_id} collection load version {load_ver}")
    if cur.count() > 1:
        raise ValueError("More than one ranks document exists in the database for "
            + f"{collection_id} collection load version {load_ver}")
    doc = await cur.next()
    return Ranks(data=doc[names.FLD_TAXA_COUNT_RANKS])


# TODO DATAPROD add counts endpoint
