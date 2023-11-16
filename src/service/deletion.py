"""
Routines to safely delete matches and selections and clean up related data in the collections system
"""

import logging
from typing import Self

from apscheduler.schedulers.background import BackgroundScheduler

from src.service.app_state_data_structures import PickleableDependencies
from src.service import models
from src.service import data_product_specs
from src.service.data_products.common_models import DataProductSpec
from src.service import processing
from src.service.storage_arango import ArangoStorage

# There's a lot of fairly similar code in this module. DRYing things up more than they are got
# too messy. Maybe take another shot later


def _logger() -> logging.Logger:
    return logging.getLogger(__name__)


class _DeletedSubset:
    # abstract away differences between matches and selections to reuse core deletion logic

    @classmethod
    async def create(
        cls,
        storage: ArangoStorage,
        delsub: models.DeletedMatch | models.DeletedSelection,
        type_: models.SubsetType
    ) -> Self:
        if cls._is_match(type_):
            active = await storage.get_match_by_internal_id(
                delsub.internal_match_id, exception=False)
        else:
            active = await storage.get_selection_by_internal_id(
                delsub.internal_selection_id, exception=False)
        return _DeletedSubset(storage, delsub, active, type_)


    def __init__(
        self,
        storage: ArangoStorage,
        deleted: models.DeletedMatch | models.DeletedSelection,
        active: models.InternalMatch | models.InternalSelection,
        type_: models.SubsetType
    ):
        self.storage = storage
        self.deleted = deleted
        self.active = active
        self.type = type_
        self.typecap = type_.value.capitalize()
        if self.is_match():
            self.id = deleted.match_id
            self.deleted_internal_id = deleted.internal_match_id
        else:
            self.id = deleted.selection_id
            self.deleted_internal_id = deleted.internal_selection_id
        self.full_id = f"{self.id}/{self.deleted_internal_id}"
    
    def is_match(self):
        return self._is_match(self.type)

    @classmethod
    def _is_match(cls, type_: models.SubsetType) -> bool:
        return type_ == models.SubsetType.MATCH

    async def remove_active_subset(self) -> bool:
        if self.is_match():
            return await self.storage.remove_match(self.id, self.active.last_access)
        else:
            return await self.storage.remove_selection(self.id, self.active.last_access)

    async def remove_deleted_subset(self):
        if self.is_match():
            await self.storage.remove_deleted_match(
                self.deleted_internal_id, self.deleted.last_access)
        else:
            await self.storage.remove_deleted_selection(
                self.deleted_internal_id, self.deleted.last_access)

    async def delete_data_product_data(self, data_product: DataProductSpec):
        if self.is_match():
            await data_product.delete_match(self.storage, self.deleted_internal_id)
        else:
            await data_product.delete_selection(self.storage, self.deleted_internal_id)


async def move_match_to_deleted_state(
    storage: ArangoStorage,
    match: models.InternalMatch,
    deletion_time_ms: int
):
    """
    Move a match to the deleted state, readying it for cleanup.

    storage - the storage system containing the match data.
    match - the match to delete.
    deletion_time_ms - the time to set as the deletion time.
    """
    delmatch = models.DeletedMatch(deleted=deletion_time_ms, **match.dict())
    _logger().info(f"Moving match {match.match_id}/{match.internal_match_id} to deleted state")
    await storage.add_deleted_match(delmatch)
    # We don't worry about whether this fails or not. The actual deletion routine will
    # clean up if there's a match both in the deleted state and non-deleted state.
    await storage.remove_match(match.match_id, match.last_access)


async def _move_matches_to_deletion(deps: PickleableDependencies, subset_age_ms: int):
    _logger().setLevel(level=logging.INFO)
    _logger().info("Marking matches for deletion")
    cli, storage = await deps.get_storage()
    try:
        cutoff = deps.get_epoch_ms() - subset_age_ms
        async def proc(m):
            await move_match_to_deleted_state(storage, m, deps.get_epoch_ms())
        await storage.process_old_matches(cutoff, proc)
    finally:
        await cli.close()


async def move_selection_to_deleted_state(
    storage: ArangoStorage,
    selection: models.InternalSelection,
    deletion_time_ms: int
):
    """
    Move a selection to the deleted state, readying it for cleanup.

    storage - the storage system containing the selection data.
    selection - the selection to delete.
    deletion_time_ms - the time to set as the deletion time.
    """
    delsel = models.DeletedSelection(deleted=deletion_time_ms, **selection.dict())
    _logger().info(f"Moving selection {delsel.selection_id}/{delsel.internal_selection_id} "
        + "to deleted state")
    await storage.add_deleted_selection(delsel)
    # We don't worry about whether this fails or not. The actual deletion routine will
    # clean up if there's a selection both in the deleted state and non-deleted state.
    await storage.remove_selection(delsel.selection_id, delsel.last_access)


async def _move_selections_to_deletion(deps: PickleableDependencies, subset_age_ms: int):
    _logger().setLevel(level=logging.INFO)
    _logger().info("Marking selections for deletion")
    cli, storage = await deps.get_storage()
    try:
        cutoff = deps.get_epoch_ms() - subset_age_ms
        async def proc(m):
            await move_selection_to_deleted_state(storage, m, deps.get_epoch_ms())
        await storage.process_old_selections(cutoff, proc)
    finally:
        await cli.close()


async def _delete_subset(
    storage: ArangoStorage,
    # pass in specs so they can be mocked without monkey patching the spec repository
    data_product_specs: dict[str, DataProductSpec],
    delsub: models.DeletedMatch | models.DeletedSelection,
    type_: models.SubsetType,
):
    delsub = await _DeletedSubset.create(storage, delsub, type_)
    # if there's no active subset with the same internal ID as the deleted subset we can proceed
    if delsub.active:
        if delsub.active.last_access == delsub.deleted.last_access:
            _logger().info(f"{delsub.typecap} {delsub.full_id} in inconsistent deletion "
                + "state, attempting to delete standard match")
            deleted = await delsub.remove_active_subset()
            if not deleted:
                _logger().info(f"{delsub.typecap} {delsub.full_id} was accessed post "
                    + "deletion, giving up.")
                return  # try again next time
        else:
            _logger().info(f"{delsub.typecap} {delsub.full_id} in inconsistent deletion state, "
                + f"attempting to delete deleted {delsub.type.value} record")
            await delsub.remove_deleted_subset()
            return  # punt either way and try again on the next go round
    # okay, the main subset document for our internal subset ID is for sure deleted so we're safe
    # to delete all the associated data
    col = await delsub.storage.get_collection_version_by_num(
        delsub.deleted.collection_id, delsub.deleted.collection_ver)
    for dpinfo in col.data_products:
        dp = data_product_specs[dpinfo.product]
        dpid = models.DataProductProcessIdentifier(
            internal_id=delsub.deleted_internal_id,
            data_product=dpinfo.product,
            type=delsub.type,
        )
        _logger().info(f"Removing data for {delsub.type.value} {delsub.full_id} "
            + f"data product {dpinfo.product}")
        await delsub.delete_data_product_data(dp)
        _logger().info(f"Removing {delsub.type.value} document for {delsub.full_id} "
            + f"data product {dpinfo.product}")
        await delsub.storage.remove_data_product_process(dpid)
    _logger().info(f"Removing {delsub.type.value} document for {delsub.full_id}")
    await delsub.remove_deleted_subset()
    

async def _delete_matches(deps: PickleableDependencies):
    _logger().setLevel(level=logging.INFO)
    _logger().info("Starting match data deletion process")
    cli, storage = await deps.get_storage()
    try:
        specs = {s.data_product: s for s in data_product_specs.get_data_products()}
        async def proc(m):
            await _delete_subset(storage, specs, m, models.SubsetType.MATCH)
        await storage.process_deleted_matches(proc)
    finally:
        await cli.close()


async def _delete_selections(deps: PickleableDependencies):
    _logger().setLevel(level=logging.INFO)
    _logger().info("Starting selection data deletion process")
    cli, storage = await deps.get_storage()
    try:
        specs = {s.data_product: s for s in data_product_specs.get_data_products()}
        async def proc(s):
            await _delete_subset(storage, specs, s, models.SubsetType.SELECTION)
        await storage.process_deleted_selections(proc)
    finally:
        await cli.close()


class SubsetCleanup:
    """
    Move matches and selections into a deleted state and when the deletion state change is
    complete execute the delection.
    """
    
    def __init__(
        self,
        pickleable_dependencies: PickleableDependencies,
        interval_sec: int = 1 * 24 * 60 * 60,
        jitter_sec: int | None = 60 * 60,
        subset_age_ms: int = 7 * 24 * 60 * 60 * 1000,
    ):
        """
        Create the subset deleter.

        pickelable_dependences - the pickleable system dependencies.
        interval_sec - how often, in seconds, the deletion scanner should run
        jitter_sec - start the job at the interval time +/- up to `jitter_sec` seconds. This allows
            for different service instances to randomize the start time, making running
            the deletion processes at the same time less likely. `jitter_sec` should be scaled
            appropriately given the value of `interval_sec`.
        subset_age_ms - how many epoch milliseconds must have elapsed since the last time a
            subset was accessed before it can be deleted. This should be set high
            enough so that any in flight requests using the subset, either subset processors
            (including data product subset processors) or subset views, can complete before the
            subset is moved to a deleted state.
        """
        self._deps = pickleable_dependencies
        self._age = subset_age_ms
        self._schd = BackgroundScheduler(daemon=True)
        self._schd.start(paused=True)
        self._started = False
        for j in [self._move_matches_to_deletion,
                  self._move_selections_to_deletion,
                  self._delete_matches,
                  self._delete_selections,
        ]:
            self._schd.add_job(j)  # run on service startup
            self._schd.add_job(j, "interval", seconds=interval_sec, jitter=jitter_sec)

    def _move_matches_to_deletion(self):
        processing.run_async_process(_move_matches_to_deletion, [self._deps, self._age])

    def _delete_matches(self):
        processing.run_async_process(_delete_matches, [self._deps])

    def _move_selections_to_deletion(self):
        processing.run_async_process(_move_selections_to_deletion, [self._deps, self._age])

    def _delete_selections(self):
        processing.run_async_process(_delete_selections, [self._deps])

    def start(self):
        """
        Start the subset deletion process.
        """
        if self._started:
            raise ValueError("already started")
        self._schd.resume()
        self._started = True

    def stop(self):
        """
        Stop the subset deletion process.
        """
        self._schd.pause()
        self._started = False
