"""
Routines to safely delete matches and clean up match data in the collections system
"""

import logging

from apscheduler.schedulers.background import BackgroundScheduler

from src.service import app_state
from src.service import models
from src.service.data_product_specs import DATA_PRODUCTS
from src.service.data_products.common_models import DataProductSpec
from src.service import processing
from src.service.storage_arango import ArangoStorage


def _logger() -> logging.Logger:
    return logging.getLogger(__name__)


async def _move_match_to_deletion(
        deps: app_state.PickleableDependencies,
        storage: ArangoStorage,
        match: models.InternalMatch
    ):
    delmatch = models.DeletedMatch(deleted=deps.get_epoch_ms(), **match.dict())
    _logger().info(f"Moving match {match.match_id} to deleted state")
    await storage.add_deleted_match(delmatch)
    # We don't worry about whether this fails or not. The actual deletion routine will
    # clean up if there's a match both in the deleted state and non-deleted state.
    await storage.remove_match(match.match_id, match.last_access)


async def _move_matches_to_deletion(deps: app_state.PickleableDependencies, match_age_ms: int):
    logging.basicConfig(level=logging.INFO)
    cli, storage = await deps.get_storage()
    try:
        cutoff = deps.get_epoch_ms() - match_age_ms
        async def proc(m):
            await _move_match_to_deletion(deps, storage, m)
        await storage.process_old_matches(cutoff, proc)
    finally:
        await cli.close()


async def _delete_match(
    storage: ArangoStorage,
    # Passing in the specs is a hack so they can be mocked without monkeypatching the real
    # repository. Longer term need to figure out a way to get them from PickleableDependencies.
    # The issue is that DataProductSpec fails to pickle for reasons I don't entirely understand
    # and updating app_state to know about DATA_PRODUCTS causes an import loop.
    # Possible solution - list strings to import in data_product_specs.py vs doing the impoarts,
    # should break the import loop. e.g. "src.service.data_products.taxa_count.TAXA_COUNT_SPEC"
    data_product_specs: dict[str, DataProductSpec],
    delmatch: models.DeletedMatch
):
    match = await storage.get_match_full(delmatch.match_id, exception=False)
    # if the internal match IDs are different it's safe to go ahead with match deletion
    if match and match.internal_match_id == delmatch.internal_match_id:
        if match.last_access == delmatch.last_access:
            _logger().info(f"Internal match {match.internal_match_id} in inconsistent deletion "
                + "state, attempting to delete standard match")
            deleted = await storage.remove_match(match.match_id, match.last_access)
            if not deleted:
                _logger().info(f"Internal match {match.internal_match_id} was accessed post "
                    + "deletion, giving up.")
                return  # try again next time
        else:
            _logger().info(f"Match {match.match_id} in inconsistent deletion state, "
                + "attempting to delete deleted match record")
            await storage.remove_deleted_match(match.internal_match_id, delmatch.last_access)
            return  # punt either way and try again on the next go round
    # okay, the main match document for our internal match ID is for sure deleted so we're safe
    # to delete all the associated data
    col = await storage.get_collection_version_by_num(
        delmatch.collection_id, delmatch.collection_ver)
    minfo = f"{delmatch.match_id}/{delmatch.internal_match_id}"
    for dpinfo in col.data_products:
        dp = data_product_specs[dpinfo.product]
        _logger().info(
            f"Removing match data for match {minfo} data product {dpinfo.product}")
        await dp.delete_match(storage, delmatch.internal_match_id)
        _logger().info(
            f"Removing match document for {minfo} data product {dpinfo.product}")
        await storage.remove_data_product_match(delmatch.internal_match_id, dpinfo.product)
    _logger().info(f"Removing match document for {minfo}")
    await storage.remove_deleted_match(delmatch.internal_match_id, delmatch.last_access)
    

async def _delete_matches(deps: app_state.PickleableDependencies):
    logging.basicConfig(level=logging.INFO)
    cli, storage = await deps.get_storage()
    try:
        async def proc(m):
            await _delete_match(storage, DATA_PRODUCTS, m)
        await storage.process_deleted_matches(proc)
    finally:
        await cli.close()


class MatchCleanup:
    """
    Move matches into a deleted state and when the deletion state change is complete
    execute the delection.
    """
    
    def __init__(
        self,
        pickleable_dependencies: app_state.PickleableDependencies,
        interval_sec: int = 1 * 24 * 60 * 60,
        jitter_sec: int | None = 60 * 60,
        match_age_ms: int = 7 * 24 * 60 * 60 * 1000,
    ):
        """
        Create the match deleter.

        pickelable_dependences - the pickleable system dependencies.
        interval_sec - how often, in seconds, the deletion scanner should run
        jitter_sec - start the job at the interval time +/- up to `jitter_sec` seconds. This allows
            for different service instances to randomize the start time, making running
            the deletion processes at the same time less likely. `jitter_sec` should be scaled
            appropriately given the value of `interval_sec`.
        match_age_ms - how many epoch milliseconds must have elapsed since the last time a
            match was accesed before it can be deleted. This should be set high
            enough so that any in flight requests using the match, either match processors
            (including data product match processors) or match views, can complete before the
            match is moved to a deleted state.
        """
        self._deps = pickleable_dependencies
        self._age = match_age_ms
        self._schd = BackgroundScheduler(daemon=True)
        self._schd.start(paused=True)
        self._started = False
        self._schd.add_job(
            self._move_matches_to_deletion,
            "interval",
            seconds=interval_sec,
            jitter=jitter_sec,
        )
        self._schd.add_job(
            self._delete_matches,
            "interval",
            seconds=interval_sec,
            jitter=jitter_sec,
        )

    def _move_matches_to_deletion(self):
        processing.run_async_process(_move_matches_to_deletion, [self._deps, self._age])

    def _delete_matches(self):
        processing.run_async_process(_delete_matches, [self._deps])

    def start(self):
        """
        Start the match deletion process.
        """
        if self._started:
            raise ValueError("already started")
        self._schd.resume()
        self._started = True

    def stop(self):
        """
        Stop the match deletion process.
        """
        self._schd.pause()
        self._started = False
