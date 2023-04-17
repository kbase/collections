"""
Reusable code for creating a heatmap based data product.
"""

import logging

from fastapi import APIRouter, Depends, Request, Query, Path

import src.common.storage.collection_and_field_names as names
from src.service import app_state
from src.service.app_state_data_structures import PickleableDependencies
from src.service.data_products import heatmap_common_models as heatmap_models
from src.service.data_products.common_functions import (
    get_load_version,
    get_load_ver_from_collection,
    get_collection_singleton_from_db,
    get_doc_from_collection_by_unique_id,
    remove_collection_keys,
    query_simple_collection_list,
    count_simple_collection_list,
    mark_data_by_kbase_id,
    remove_marked_subset,
)
from src.service.data_products.common_models import (
    DataProductSpec,
    DBCollection,
    QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE,
    QUERY_VALIDATOR_LIMIT,
    QUERY_COUNT,
    QUERY_MATCH_ID,
    QUERY_MATCH_MARK,
    QUERY_SELECTION_ID,
    QUERY_SELECTION_MARK,
    QUERY_STATUS_ONLY,
)
from src.service.http_bearer import KBaseHTTPBearer
from src.service import kb_auth
from src.service import models
from src.service import processing_matches
from src.service import processing_selections
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
        )
        router.add_api_route(
            "/cell/{cell_id}",
            self.get_cell,
            methods=["GET"],
            response_model=heatmap_models.CellDetail,
            summary=f"Get a cell in a {self._api_category} heatmap",
            description=f"Get detailed information about a cell in a {self._api_category} heatmap."
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
                    indexes=[
                        [
                            names.FLD_COLLECTION_ID,
                            names.FLD_LOAD_VERSION,
                            names.FLD_KBASE_ID
                        ],
                        [
                            names.FLD_COLLECTION_ID,
                            names.FLD_LOAD_VERSION,
                            # https://www.arangodb.com/docs/stable/indexing-index-basics.html#indexing-array-values
                            names.FLD_MATCHES_SELECTIONS + "[*]",
                            names.FLD_KBASE_ID
                            # for finding matches/selections, and opt a default sort on the kbase ID
                        ],
                        [names.FLD_MATCHES_SELECTIONS + "[*]"]  # for deletions
                    ]
                ),
            ]
        )

    async def _delete_match(self, storage: ArangoStorage, internal_match_id: str):
        await remove_marked_subset(
            storage, self._colname_data, _MATCH_ID_PREFIX + internal_match_id)

    async def _delete_selection(self, storage: ArangoStorage, internal_selection_id: str):
        await remove_marked_subset(
            storage, self._colname_data, _SELECTION_ID_PREFIX + internal_selection_id)

    async def get_meta_info(
        self,
        r: Request,
        collection_id: str = PATH_VALIDATOR_COLLECTION_ID,
        load_ver_override: str | None = QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE,
        user: kb_auth.KBaseUser = Depends(_OPT_AUTH)
    ) -> heatmap_models.HeatMapMeta:
        storage = app_state.get_app_state(r).arangostorage
        load_ver = await get_load_version(
            storage, collection_id, self._id, load_ver_override, user)
        doc = await get_collection_singleton_from_db(
            storage, self._colname_meta, collection_id, load_ver, bool(load_ver_override))
        return heatmap_models.HeatMapMeta.construct(**remove_collection_keys(doc))

    async def get_cell(
        self,
        r: Request,
        collection_id: str = PATH_VALIDATOR_COLLECTION_ID,
        cell_id: str = Path(
            example="4",
            description="The ID of the cell in the heatmap."
        ),
        load_ver_override: str | None = QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE,
        user: kb_auth.KBaseUser = Depends(_OPT_AUTH)
    ) -> heatmap_models.CellDetail:
        storage = app_state.get_app_state(r).arangostorage
        load_ver = await get_load_version(
            storage, collection_id, self._id, load_ver_override, user)
        doc = await get_doc_from_collection_by_unique_id(
            storage, self._colname_cells, collection_id, load_ver, cell_id, "cell detail", True,
        )
        return heatmap_models.CellDetail.construct(**remove_collection_keys(doc))

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
        match_id: str | None = QUERY_MATCH_ID,
        # TODO FEATURE support a choice of AND or OR for matches & selections
        match_mark: bool = QUERY_MATCH_MARK,
        selection_id: str | None = QUERY_SELECTION_ID,
        selection_mark: bool = QUERY_SELECTION_MARK,
        status_only: bool = QUERY_STATUS_ONLY,
        load_ver_override: str | None = QUERY_VALIDATOR_LOAD_VERSION_OVERRIDE,
        user: kb_auth.KBaseUser = Depends(_OPT_AUTH)
    ) -> heatmap_models.HeatMap:
        appstate = app_state.get_app_state(r)
        dp_match, dp_sel = None, None
        if match_id or selection_id:
            coll = await appstate.arangostorage.get_collection_active(collection_id)
            load_ver = get_load_ver_from_collection(coll, self._id)
        else:
            load_ver = await get_load_version(
                appstate.arangostorage, collection_id, self._id, load_ver_override, user)
        if match_id:
            dp_match = await processing_matches.get_or_create_data_product_match_process(
                appstate, coll, user, match_id, self._id, self._process_heatmap_subset
            )
        if selection_id:
            dp_sel = await processing_selections.get_or_create_data_product_selection_process(
                appstate, coll, selection_id, self._id, self._process_heatmap_subset
            )
        if status_only:
            return self._heatmap(dp_match=dp_match, dp_sel=dp_sel)
        elif count:
            count = await self._count(
                appstate.arangostorage,
                collection_id,
                load_ver,
                self._get_complete_internal_id(dp_match) if not match_mark else None,
                self._get_complete_internal_id(dp_sel) if not selection_mark else None,
            )
            return self._heatmap(dp_match=dp_match, dp_sel=dp_sel, count=count)
        else:
            return await self._query(  # may want a sort direction arg?
                appstate.arangostorage,
                collection_id,
                load_ver,
                start_after,
                limit,
                match_proc=dp_match,
                match_mark=match_mark,
                selection_proc=dp_sel,
                selection_mark=selection_mark,
            )
    
    async def _process_heatmap_subset(
        self,
        deps: PickleableDependencies,
        storage: ArangoStorage,
        match_or_sel: models.InternalMatch | models.InternalSelection,
        coll: models.SavedCollection,
        dpid: models.DataProductProcessIdentifier,
    ):
        load_ver = {dp.product: dp.version for dp in coll.data_products}[self._id]
        missed = await mark_data_by_kbase_id(
            storage,
            self._colname_data,
            coll.id,
            load_ver,
            match_or_sel.matches if dpid.is_match() else match_or_sel.selection_ids,
            (_MATCH_ID_PREFIX if dpid.is_match() else _SELECTION_ID_PREFIX) + dpid.internal_id,
        )
        # since this is a secondary match the process shouldn't fail since the match / selection
        # was already applied to another data product, but we should report just in case
        state = models.ProcessState.FAILED if missed else models.ProcessState.COMPLETE
        if missed:
            logging.getLogger(__name__).warn(
                f"{dpid.type.value} process with internal ID {dpid.internal_id} failed due to "
                + f"missing data IDs: {missed}"
            )
        await storage.update_data_product_process_state(dpid, state, deps.get_epoch_ms())

    def _heatmap(
        self,
        dp_match: models.DataProductProcess = None,
        dp_sel: models.DataProductProcess = None,
        count: int = None,
        data: list[heatmap_models.HeatMapRow] = None,
        min_value: int = None,
        max_value: int = None,
    ) -> heatmap_models.HeatMap:
        return heatmap_models.HeatMap(
            heatmap_match_state=dp_match.state if dp_match else None,
            heatmap_selection_state=dp_sel.state if dp_sel else None,
            count=count,
            data=data,
            min_value=min_value,
            max_value=max_value,
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
        return count

    def _get_complete_internal_id(self, dp_proc: models.DataProductProcess | None) -> str:
        return dp_proc.internal_id if dp_proc and dp_proc.is_complete() else None

    async def _query(
        # ew. too many args
        self,
        store: ArangoStorage,
        collection_id: str,
        load_ver: str,
        start_after: str,
        limit: int,
        match_proc: models.DataProductProcess | None,
        match_mark: bool,
        selection_proc: models.DataProductProcess | None,
        selection_mark: bool,
    ) -> heatmap_models.HeatMap:
        internal_match_id = self._get_complete_internal_id(match_proc)
        internal_selection_id = self._get_complete_internal_id(selection_proc)
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
        return self._heatmap(
            dp_match=match_proc,
            dp_sel=selection_proc,
            data=data,
            min_value=min(vals) if vals else None,
            max_value=max(vals) if vals else None
        )
