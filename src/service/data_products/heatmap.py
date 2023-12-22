"""
Reusable code for creating a heatmap based data product.
"""

import json
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Request, Query, Path, Response

import src.common.storage.collection_and_field_names as names
from src.common.product_models import columnar_attribs_common_models as col_models
from src.common.product_models.common_models import FIELD_MATCH_STATE, FIELD_SELECTION_STATE
from src.common.product_models import heatmap_common_models as heatmap_models
from src.service import app_state, kb_auth, models
from src.service.data_products.common_functions import (
    get_load_version,
    get_collection_singleton_from_db,
    get_doc_from_collection_by_unique_id,
    remove_collection_keys,
    query_simple_collection_list,
    remove_marked_subset,
)
from src.service.data_products.common_models import (
    DataProductSpec,
    DBCollection,
    DataProductMissingIDs,
    QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE,
    QUERY_VALIDATOR_LIMIT,
    QUERY_VALIDATOR_COUNT,
    QUERY_VALIDATOR_MATCH_ID,
    QUERY_VALIDATOR_MATCH_MARK,
    QUERY_VALIDATOR_SELECTION_ID,
    QUERY_VALIDATOR_SELECTION_MARK,
    QUERY_VALIDATOR_STATUS_ONLY,
)
from src.service.data_products.data_product_processing import (
    MATCH_ID_PREFIX,
    SELECTION_ID_PREFIX,
    get_load_version_and_processes,
    get_missing_ids,
)
from src.service.filtering.filtering_processing import get_filters, FILTER_STRATEGRY_TEXT
from src.service.filtering.filters import FilterSet
from src.service.filtering.generic_view import get_generic_view_name
from src.service.http_bearer import KBaseHTTPBearer
from src.service.processing import SubsetSpecification
from src.service.routes_common import PATH_VALIDATOR_COLLECTION_ID
from src.service.storage_arango import ArangoStorage, remove_arango_keys


_OPT_AUTH = KBaseHTTPBearer(optional=True)

# Default string columns present in heatmap row data but not existing in the HeatMapMeta
ID_COLS = [names.FLD_KBASE_ID, names.FLD_LOAD_VERSION, names.FLD_COLLECTION_ID,
           names.FLD_MATCHES_SELECTIONS]
NGRAM_COLS = [names.FLD_KB_DISPLAY_NAME]


def _bools_to_ints(list_: list):
    return [int(item) if isinstance(item, bool) else item for item in list_]


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
        cell_detail_collection_name: str,
    ):
        """
        Initialize the controller.

        heatmap_id - the heat map data product ID. This will appear in the endpoint urls for the
            heatmap data.
        api_category - the category in the API documents where the heatmap endpoints will be
            grouped.
        meta_collection_name - the name of the arango collection containing heatmap metadata.
        data_collection_name - the name of the arango collection containing the heatmap data.
        cell_detail_collection_name - the name of the arango collection containing the detailed
            information for the cells in the heatmap.
        """
        self._id = heatmap_id
        self._colname_meta = meta_collection_name
        self._colname_data = data_collection_name
        self._colname_cells = cell_detail_collection_name
        self._api_category = api_category
        # This class needs to be pickleable so only create the data product spec on demand
        # and don't make it part of the state

    def _get_filtering_text(self) -> str:
        return f"""

**FILTERING:**

The returned data can be filtered by column content by adding query parameters of the format
```
filter_<column id>=<filter criteria>
```
For example:
```
GET <host>/collections/PMI/data_products/{self._id}/?filter_1=[0,2]
GET <host>/collections/PMI/data_products/{self._id}/?filter_49=true
```

For metadata columns such as '{ID_COLS[0]}' and '{NGRAM_COLS[0]}', the filter format shifts to utilizing the 
column name rather than the column ID. 
```
filter_<column name>=<filter criteria>
```
For example:
```
GET <host>/collections/PMI/data_products/{self._id}/?filter_kbase_id=69278_1006_1
```
""" + FILTER_STRATEGRY_TEXT

    def _create_router(self) -> APIRouter:
        router = APIRouter(tags=[self._api_category], prefix=f"/{self._id}")
        router.add_api_route(
            "/meta",
            self.get_meta_info,
            methods=["GET"],
            response_model=heatmap_models.HeatMapMeta,
            summary=f"Get {self._api_category} metadata",
            description=f"Get meta information about the data in the {self._api_category} heatmap, "
                + "such as column names and descriptions, value ranges, etc."
        )
        router.add_api_route(
            "/",
            self.get_heatmap,
            methods=["GET"],
            response_model=heatmap_models.HeatMap,
            summary=f"Get {self._api_category} heatmap data",
            description=f"Get data in the {self._api_category} heatmap.\n\n"
                + "Authentication is not required unless submitting a match ID or "
                + "overriding the load version; in the latter case service administration "
                + "permissions are required.\n\n"
                + "When creating selections from genome attributes, use the "
                + f"`{names.FLD_KBASE_ID}` field values as input."
                + self._get_filtering_text()
        )
        router.add_api_route(
            "/cell/{cell_id}",
            self.get_cell,
            methods=["GET"],
            response_model=heatmap_models.CellDetail,
            summary=f"Get a cell in a {self._api_category} heatmap",
            description=f"Get detailed information about a cell in a {self._api_category} heatmap."
        )
        router.add_api_route(
            "/missing",
            self.get_missing_ids,
            methods=["GET"],
            response_model=DataProductMissingIDs,
            summary=f"Get missing IDs for a match or selection",
            description=f"Get the list of IDs that were not found in this {self._api_category} "
                + "heatmap but were present in the match and / or selection.",
        )
        return router

    def get_data_product_spec(self) -> DataProductSpec:
        router = self._create_router()
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
                DBCollection(name=self._colname_meta, indexes=[]),  # just use the doc key
                DBCollection(name=self._colname_cells, indexes=[]),  # just use the doc key
                DBCollection(
                    name=self._colname_data,
                    generic_view_required=True,
                    indexes=[
                        [
                            names.FLD_COLLECTION_ID,
                            names.FLD_LOAD_VERSION,
                            names.FLD_KBASE_ID,
                            # for applying matches and selections
                        ],
                        [
                            names.FLD_COLLECTION_ID,
                            names.FLD_LOAD_VERSION,
                            names.FLD_KB_DISPLAY_NAME,
                            # for sorting on display name, which is currently hardcoded
                        ],
                        [
                            names.FLD_COLLECTION_ID,
                            names.FLD_LOAD_VERSION,
                            # https://www.arangodb.com/docs/stable/indexing-index-basics.html#indexing-array-values
                            names.FLD_MATCHES_SELECTIONS + "[*]",
                            names.FLD_KB_DISPLAY_NAME
                            # for finding matches/selections, and opt a default sort on the
                            # display name
                        ],
                        [names.FLD_MATCHES_SELECTIONS + "[*]"]  # for deletions
                    ]
                ),
            ]
        )

    async def _delete_match(self, storage: ArangoStorage, internal_match_id: str):
        await remove_marked_subset(
            storage, self._colname_data, MATCH_ID_PREFIX + internal_match_id)

    async def _delete_selection(self, storage: ArangoStorage, internal_selection_id: str):
        await remove_marked_subset(
            storage, self._colname_data, SELECTION_ID_PREFIX + internal_selection_id)

    async def get_meta_info(
        self,
        r: Request,
        collection_id: Annotated[str, PATH_VALIDATOR_COLLECTION_ID],
        load_ver_override: QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE = None,
        user: kb_auth.KBaseUser = Depends(_OPT_AUTH)
    ) -> heatmap_models.HeatMapMeta:
        storage = app_state.get_app_state(r).arangostorage
        _, load_ver = await get_load_version(
            storage, collection_id, self._id, load_ver_override, user)
        doc = await get_collection_singleton_from_db(
            storage, self._colname_meta, collection_id, load_ver, bool(load_ver_override))
        return heatmap_models.HeatMapMeta(**remove_collection_keys(doc))

    async def get_cell(
        self,
        r: Request,
        collection_id: Annotated[str, PATH_VALIDATOR_COLLECTION_ID],
        cell_id: str = Path(
            example="4",
            description="The ID of the cell in the heatmap."
        ),
        load_ver_override: QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE = None,
        user: kb_auth.KBaseUser = Depends(_OPT_AUTH)
    ) -> heatmap_models.CellDetail:
        storage = app_state.get_app_state(r).arangostorage
        _, load_ver = await get_load_version(
            storage, collection_id, self._id, load_ver_override, user)
        doc = await get_doc_from_collection_by_unique_id(
            storage, self._colname_cells, collection_id, load_ver, cell_id, "cell detail", True,
        )
        return heatmap_models.CellDetail(**remove_collection_keys(doc))

    def _append_col(
            self,
            columns: list[col_models.AttributesColumn],
            col_list: list[str],
            column_type: col_models.ColumnType,
            filter_strategy: col_models.FilterStrategy) -> None:
        # create AttributesColumn objects from a list of column names and add them, in place, to the given columns list
        for col_name in col_list:
            columns.append(col_models.AttributesColumn(
                key=col_name,
                type=column_type,
                filter_strategy=filter_strategy,
            ))

    async def _get_heatmap_columns(
            self,
            storage: ArangoStorage,
            coll_id: str,
            load_ver: str,
            load_ver_override: bool
    ) -> list[col_models.AttributesColumn]:
        # Retrieve a list of AttributesColumn objects derived from the ColumnInformation objects within HeatMapMeta.
        # Additionally, include columns that exist in the heatmap row data but are not present in HeatMapMeta.

        doc = await get_collection_singleton_from_db(
            storage, self._colname_meta, coll_id, load_ver, bool(load_ver_override))
        column_meta = heatmap_models.HeatMapMeta(**remove_collection_keys(doc))

        columns = [heatmap_models.transfer_col_heatmap_to_attribs(col)
                   for category in column_meta.categories for col in category.columns]

        # append columns existing in the heatmap row data but not in the HeatMapMeta
        self._append_col(columns, ID_COLS, col_models.ColumnType.STRING, col_models.FilterStrategy.IDENTITY)
        self._append_col(columns, NGRAM_COLS, col_models.ColumnType.STRING, col_models.FilterStrategy.NGRAM)

        return columns

    def _trans_field_func(self, field_name: str) -> str:
        # Transforms the field name into a valid column name extracted from the filter query.
        # For instance, converts a query field name '1' into a valid column name 'col_1_val'.

        return heatmap_models.form_heatmap_cell_val_key(field_name) if field_name.isdigit() else field_name

    async def get_heatmap(
        self,
        r: Request,
        collection_id: Annotated[str, PATH_VALIDATOR_COLLECTION_ID],
        start_after: str = Query(
            default=None,
            example="my_object_name",
            description=f"The `{names.FLD_KB_DISPLAY_NAME}` to start after when listing data. "
                + "This parameter can be used to page through the data by providing the ID from "
                + "the last row in the previous set of data."
        ),
        limit: QUERY_VALIDATOR_LIMIT = 1000,
        count: QUERY_VALIDATOR_COUNT = False,
        match_id: QUERY_VALIDATOR_MATCH_ID = None,
        # TODO FEATURE support a choice of AND or OR for matches & selections
        match_mark: QUERY_VALIDATOR_MATCH_MARK = False,
        selection_id: QUERY_VALIDATOR_SELECTION_ID = None,
        selection_mark: QUERY_VALIDATOR_SELECTION_MARK = False,
        status_only: QUERY_VALIDATOR_STATUS_ONLY = False,
        load_ver_override: QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE = None,
        user: kb_auth.KBaseUser = Depends(_OPT_AUTH)
    ) -> Response:
        # For some reason returning the data as a model slows down the endpoint by ~10x.
        # Serializing manually and returning a plain response is much faster
        appstate = app_state.get_app_state(r)
        load_ver, dp_match, dp_sel = await get_load_version_and_processes(
            appstate,
            user,
            self._colname_data,
            collection_id,
            self._id,
            load_ver_override=load_ver_override,
            match_id=match_id,
            selection_id=selection_id,
        )
        if status_only:
            return self._response(dp_match=dp_match, dp_sel=dp_sel)
        columns = await self._get_heatmap_columns(appstate.arangostorage, collection_id, load_ver, load_ver_override)
        filters = await get_filters(
            r,
            arango_coll=self._colname_data,
            coll_id=collection_id,
            load_ver=load_ver,
            load_ver_override=load_ver_override,
            data_product=self._id,
            columns=columns,
            view_name=get_generic_view_name(self._id),
            count=count,
            sort_on=names.FLD_KB_DISPLAY_NAME,
            sort_desc=False,
            match_spec=SubsetSpecification(
                subset_process=dp_match, mark_only=match_mark, prefix=MATCH_ID_PREFIX),
            selection_spec=SubsetSpecification(
                subset_process=dp_sel, mark_only=selection_mark, prefix=SELECTION_ID_PREFIX),
            start_after=start_after,
            limit=limit,
            trans_field_func=self._trans_field_func
        )
        return await self._query(
            appstate.arangostorage, filters, match_proc=dp_match, selection_proc=dp_sel)

    async def get_missing_ids(
        self,
        r: Request,
        collection_id: Annotated[str, PATH_VALIDATOR_COLLECTION_ID],
        match_id: Annotated[str, Query(description="A match ID.")] = None,
        selection_id: Annotated[str, Query(description="A selection ID.")] = None,
        user: kb_auth.KBaseUser = Depends(_OPT_AUTH),
    ) -> DataProductMissingIDs:
        return await get_missing_ids(
            app_state.get_app_state(r),
            self._colname_data,
            collection_id,
            self._id,
            match_id=match_id,
            selection_id=selection_id,
            user=user,
        )

    def _response(
        self,
        dp_match: models.DataProductProcess = None,
        dp_sel: models.DataProductProcess = None,
        count: int = None,
        data: list[heatmap_models.HeatMapRow] = None,
        min_value: int = None,
        max_value: int = None,
    ) -> Response:
        j = {
            FIELD_MATCH_STATE: dp_match.state if dp_match else None,
            FIELD_SELECTION_STATE: dp_sel.state if dp_sel else None,
            heatmap_models.FIELD_HEATMAP_DATA: data,
            heatmap_models.FIELD_HEATMAP_MIN_VALUE: min_value,
            heatmap_models.FIELD_HEATMAP_MAX_VALUE: max_value,
            heatmap_models.FIELD_HEATMAP_COUNT: count,
        }
        return Response(content=json.dumps(j), media_type="application/json")
    
    def _remove_doc_keys(self, doc: dict[str, Any]) -> dict[str, Any]:
        # removes in place
        doc = remove_arango_keys(remove_collection_keys(doc))
        doc.pop(names.FLD_MATCHES_SELECTIONS, None)

        heatmap_models.revert_transformed_heatmap_row_cells(doc)

        return doc

    async def _query(
        self,
        store: ArangoStorage,
        filters: FilterSet,
        match_proc: models.DataProductProcess | None,
        selection_proc: models.DataProductProcess | None,
    ) -> Response:
        data = []
        await query_simple_collection_list(
            store,
            filters,
            lambda doc: data.append(doc) if filters.count else
                data.append(self._remove_doc_keys(doc)),
        )
        if filters.count:
            return self._response(dp_match=match_proc, dp_sel=selection_proc, count=data[0])
        else:
            vals = set()
            for r in data:  # lazy lazy lazy
                vals |= {c[heatmap_models.FIELD_HEATMAP_CELL_VALUE]
                         for c in r[heatmap_models.FIELD_HEATMAP_ROW_CELLS]}
            return self._response(
                dp_match=match_proc,
                dp_sel=selection_proc,
                data=data,
                min_value=min(_bools_to_ints(vals)) if vals else None,
                max_value=max(_bools_to_ints(vals)) if vals else None
            )
