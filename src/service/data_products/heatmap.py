"""
Reusable code for creating a heatmap based data product.
"""

from fastapi import APIRouter, Depends, Request, Query

import src.common.storage.collection_and_field_names as names
from src.service import app_state
from src.service.data_products import heatmap_common_models as heatmap_models
from src.service.data_products.common_functions import (
    get_load_version,
    get_load_ver_from_collection,
    get_collection_singleton_from_db,
    remove_collection_keys,
    query_simple_collection_list,
    count_simple_collection_list,
)
from src.service.data_products.common_models import (
    DataProductSpec,
    DBCollection,
    QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE,
    QUERY_VALIDATOR_LIMIT,
    QUERY_COUNT,
)
from src.service.http_bearer import KBaseHTTPBearer
from src.service import kb_auth
from src.service.routes_common import PATH_VALIDATOR_COLLECTION_ID
from src.service.storage_arango import ArangoStorage

_OPT_AUTH = KBaseHTTPBearer(optional=True)

_MATCH_ID_PREFIX = "m_"
_SELECTION_ID_PREFIX = "s_"


def _prefix_id(prefix: str, id_: str | None) -> str | None:
    return prefix + id_ if id_ else None


class HeatMapController:
    """
    A controller for creating a set of heat map endpoints.
    """

    def __init__(
        self,
        heatmap_id: str,
        api_category: str,
        meta_collection_name: str,
        data_collection_name: str,
    ):
        """
        Initialize the controller.

        heatmap_id - the heat map data product ID. This will appear in the endpoint urls for the
            heatmap data.
        api_category - the category in the API documents where the heatmap endpoints will be
            grouped.
        meta_collection_name - the name of the arango collection containing heatmap metadata.
        data_collection_name - the name of the arango collection containing the heatmap data.
        """
        self._id = heatmap_id
        self._colname_meta = meta_collection_name
        self._colname_data = data_collection_name
        router = self._create_router(api_category)
        self.data_product_spec = self._create_data_product_spec(router)

    def _create_router(self, api_category: str) -> APIRouter:
        router = APIRouter(tags=[api_category], prefix=f"/{self._id}")
        router.add_api_route(
            "/meta",
            self.get_meta_info,
            methods=["GET"],
            response_model=heatmap_models.HeatMapMeta,
            summary=f"Get {api_category} metadata",
            description=f"Get meta information about the data in the {api_category} heatmap, "
                + "such as column names and descriptions, value ranges, etc."
        )
        router.add_api_route(
            "/",
            self.get_heatmap,
            methods=["GET"],
            response_model=heatmap_models.HeatMap,
            summary=f"Get {api_category} heatmap data",
            description=f"Get data in the {api_category} heatmap."
        )
        return router

    def _create_data_product_spec(self, router: APIRouter) -> DataProductSpec:
        outerself = self

        class HeatMapDataProductSpec(DataProductSpec):
            """
            The heat map specific data product specification.
            """

            async def delete_match(self, storage: ArangoStorage, internal_match_id: str):
                """
                Delete heatmap match data.

                storage - the storage system
                internal_match_id - the match to delete.
                """
                await outerself._delete_match(storage, internal_match_id)

            async def delete_selection(self, storage: ArangoStorage, internal_selection_id: str):
                """
                Delete heatmap selection data.

                storage - the storage system
                internal_selection_id - the selection to delete.
                """
                await outerself._delete_selection(storage, internal_selection_id)

        return HeatMapDataProductSpec(
            data_product=self._id,
            router=router,
            db_collections=[
                DBCollection(
                    name=self._colname_meta,
                    indexes=[]  # just use the doc key
                ),
                DBCollection(
                    name=self._colname_data,
                    indexes=[
                        [
                            names.FLD_COLLECTION_ID,
                            names.FLD_LOAD_VERSION,
                            names.FLD_KBASE_ID
                        ],
                        # TODO HEATMAP will need an index for matches & selections
                    ]
                ),
                # TODO HEATMAP heatmap cell data collection
            ]
        )

    async def _delete_match(self, storage: ArangoStorage, internal_match_id: str):
        # TODO HEATMAP delete matches
        print("Totally deleting matches right now", storage, internal_match_id)

    async def _delete_selection(self, storage: ArangoStorage, internal_selection_id: str):
        # TODO HEATMAP delete selections
        print("There go the selections. Really. Not kidding at all", storage, internal_selection_id)

    async def get_meta_info(
        self,
        r: Request,
        collection_id: str = PATH_VALIDATOR_COLLECTION_ID,
        load_ver_override: str | None = QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE,
        user: kb_auth.KBaseUser = Depends(_OPT_AUTH)
    ) -> heatmap_models.HeatMapMeta:
        appstate = app_state.get_app_state(r)
        storage = appstate.arangostorage
        load_ver = await get_load_version(
            storage, collection_id, self._id, load_ver_override, user)
        doc = await get_collection_singleton_from_db(
            storage, self._colname_meta, collection_id, load_ver, bool(load_ver_override))
        return heatmap_models.HeatMapMeta.construct(**remove_collection_keys(doc))

    async def get_heatmap(
        self,
        r: Request,
        collection_id: str = PATH_VALIDATOR_COLLECTION_ID,
        start_after: str | None = Query(
            default=None,
            example="GB_GCA_000006155.2",
            description=f"The `{names.FLD_KBASE_ID}` to start after when listing data. This "
                + "parameter can be used to page through the data by providing the ID from "
                + "the last row in the previous set of data."
        ),
        limit: int = QUERY_VALIDATOR_LIMIT,
        count: bool = QUERY_COUNT,
        load_ver_override: str | None = QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE,
        user: kb_auth.KBaseUser = Depends(_OPT_AUTH)
    ) -> heatmap_models.HeatMap:
        appstate = app_state.get_app_state(r)
        storage = appstate.arangostorage
        load_ver = await get_load_version(
            storage, collection_id, self._id, load_ver_override, user)
        if count:
            return await self._count(storage, collection_id, load_ver, None, None)
        else:
            return await self._query(  # may want a sort direction arg?
                storage,
                collection_id,
                load_ver,
                start_after,
                limit,
                None,
                False,
                None,
                False,
            ) 
    
    async def _count(
        self,
        store: ArangoStorage,
        collection_id: str,
        load_ver: str,
        internal_match_id: str | None,
        internal_selection_id: str | None,
    ):
        # for now this method doesn't do much. One we have some filtering implemented
        # it'll need to take that into account.
        count = await count_simple_collection_list(
            store,
            self._colname_data,
            collection_id,
            load_ver,
            internal_match_id=_prefix_id(_MATCH_ID_PREFIX, internal_match_id),
            internal_selection_id=_prefix_id(_SELECTION_ID_PREFIX, internal_selection_id),
        )
        return heatmap_models.HeatMap(count=count)

    async def _query(
        # ew. too many args
        self,
        store: ArangoStorage,
        collection_id: str,
        load_ver: str,
        start_after: str,
        limit: int,
        internal_match_id: str | None,
        match_mark: bool,
        internal_selection_id: str | None,
        selection_mark: bool,
    ) -> heatmap_models.HeatMap:
        data = []
        await query_simple_collection_list(
            store,
            self._colname_data,
            lambda doc: data.append(heatmap_models.HeatMapRow.parse_obj(
                remove_collection_keys(doc))),
            collection_id,
            load_ver,
            names.FLD_KBASE_ID,
            sort_descending=False,
            skip=0,
            start_after=start_after,
            limit=limit,
            internal_match_id=_prefix_id(_MATCH_ID_PREFIX, internal_match_id),
            match_mark=match_mark,
            internal_selection_id=_prefix_id(_SELECTION_ID_PREFIX, internal_selection_id),
            selection_mark=selection_mark,    
        )
        vals = []
        for r in data:  # lazy lazy lazy
            vals += [c.val for c in r.cells]
        return heatmap_models.HeatMap(
            data=data,
            min_value=min(vals) if vals else None,
            max_value=max(vals) if vals else None
        )
