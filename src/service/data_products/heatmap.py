"""
Reusable code for creating a heatmap based data product.
"""

from fastapi import APIRouter

import src.common.storage.collection_and_field_names as names
from src.service.data_products import heatmap_common_models as heatmap_models
from src.service.data_products.common_models import (
    DataProductSpec,
    DBCollection,
    QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE,
)
from src.service.storage_arango import ArangoStorage


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
        self._router = APIRouter(tags=[api_category], prefix=f"/{heatmap_id}")
        self._router.add_api_route(
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
            router=self._router,
            db_collections=[
                DBCollection(
                    name=column_collection_name,
                    indexes=[
                        [
                            names.FLD_COLLECTION_ID,
                            names.FLD_LOAD_VERSION,
                        ]
                    ]
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

    async def get_column_info(self) -> heatmap_models.Columns:
        """
        Get information about the columns in the heatmap.
        """
        print("getting columns right now, here you go")
        return heatmap_models.Columns(categories=[])  # TODO HEATMAP return real column data
