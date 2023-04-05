"""
Reusable code for creating a heatmap based data product.
"""

from fastapi import APIRouter, Depends, Request

import src.common.storage.collection_and_field_names as names
from src.service import app_state
from src.service.data_products import heatmap_common_models as heatmap_models
from src.service.data_products.common_functions import (
    get_load_version,
    get_load_ver_from_collection,
    get_collection_singleton_from_db
)
from src.service.data_products.common_models import (
    DataProductSpec,
    DBCollection,
    QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE,
)
from src.service.http_bearer import KBaseHTTPBearer
from src.service import kb_auth
from src.service.routes_common import PATH_VALIDATOR_COLLECTION_ID
from src.service.storage_arango import ArangoStorage

_OPT_AUTH = KBaseHTTPBearer(optional=True)


class HeatMapController:
    """
    A controller for creating a set of heat map endpoints.
    """

    def __init__(self, heatmap_id: str, api_category: str, column_collection_name: str):
        """
        Initialize the controller.

        heatmap_id - the heat map data product ID. This will appear in the endpoint urls for the
            heatmap data.
        api_category - the category in the API documents where the heatmap endpoints will be
            grouped.
        column_collection_name - the name of the arango collection containing information about
            the columns in the heatmap.
        """
        self._id = heatmap_id
        self._colname_columns = column_collection_name
        router = APIRouter(tags=[api_category], prefix=f"/{heatmap_id}")
        router.add_api_route(
            "/columns",
            self.get_column_info,
            methods=["GET"],
            response_model=heatmap_models.Columns,
            summary=f"Get {api_category} columns",
            description=f"Get information about the columns in the {api_category} heatmap."
        )

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

        self.data_product_spec = HeatMapDataProductSpec(
            data_product=self._id,
            router=router,
            db_collections=[
                DBCollection(
                    name=column_collection_name,
                    indexes=[]  # just use the doc key
                )
                # TODO HEATMAP main heatmap data collection
                # TODO HEATMAP heatmap cell data collection
            ]
        )

    async def _delete_match(self, storage: ArangoStorage, internal_match_id: str):
        # TODO HEATMAP delete matches
        print("Totally deleting matches right now", storage, internal_match_id)

    async def _delete_selection(self, storage: ArangoStorage, internal_selection_id: str):
        # TODO HEATMAP delete selections
        print("There go the selections. Really. Not kidding at all", storage, internal_selection_id)

    async def get_column_info(
        self,
        r: Request,
        collection_id: str = PATH_VALIDATOR_COLLECTION_ID,
        load_ver_override: str | None = QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE,
        user: kb_auth.KBaseUser = Depends(_OPT_AUTH)
    ) -> heatmap_models.Columns:
        appstate = app_state.get_app_state(r)
        storage = appstate.arangostorage
        load_ver = await get_load_version(
            storage, collection_id, self._id, load_ver_override, user)
        doc = await get_collection_singleton_from_db(
            storage, self._colname_columns, collection_id, load_ver, bool(load_ver_override))
        return heatmap_models.Columns(categories=doc[names.FLD_HEATMAP_COLUMN_CATEGORIES])
