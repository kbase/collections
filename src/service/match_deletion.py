"""
Routines to safely delete matches and clean up match data in the collections system
"""

import logging

from apscheduler.schedulers.background import BackgroundScheduler

from src.service.app_state_data_structures import PickleableDependencies
from src.service import models
from src.service import data_product_specs
from src.service.data_products.common_models import DataProductSpec
from src.service import processing
from src.service.storage_arango import ArangoStorage


def _logger() -> logging.Logger:
    return logging.getLogger(__name__)


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


async def _move_matches_to_deletion(deps: PickleableDependencies, match_age_ms: int):
    logging.basicConfig(level=logging.INFO)
    _logger().info("Marking matches for deletion")
    cli, storage = await deps.get_storage()
    try:
        cutoff = deps.get_epoch_ms() - match_age_ms
        async def proc(m):
            await move_match_to_deleted_state(storage, m, deps.get_epoch_ms())
        await storage.process_old_matches(cutoff, proc)
    finally:
        await cli.close()


async def _delete_match(
    storage: ArangoStorage,
    # pass in specs so they can be mocked without monkey patching the spec repository
    data_product_specs: dict[str, DataProductSpec],
    delmatch: models.DeletedMatch
):
    match = await storage.get_match_full(delmatch.match_id, exception=False)
    minfo = f"{delmatch.match_id}/{delmatch.internal_match_id}"
    # if the internal match IDs are different it's safe to go ahead with match deletion
    if match and match.internal_match_id == delmatch.internal_match_id:
        if match.last_access == delmatch.last_access:
            _logger().info(f"Match {minfo} in inconsistent deletion "
                + "state, attempting to delete standard match")
            deleted = await storage.remove_match(match.match_id, match.last_access)
            if not deleted:
                _logger().info(f"Match {minfo} was accessed post "
                    + "deletion, giving up.")
                return  # try again next time
        else:
            _logger().info(f"Match {minfo} in inconsistent deletion state, "
                + "attempting to delete deleted match record")
            await storage.remove_deleted_match(match.internal_match_id, delmatch.last_access)
            return  # punt either way and try again on the next go round
    # okay, the main match document for our internal match ID is for sure deleted so we're safe
    # to delete all the associated data
    col = await storage.get_collection_version_by_num(
        delmatch.collection_id, delmatch.collection_ver)
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
    

async def _delete_matches(deps: PickleableDependencies):
    logging.basicConfig(level=logging.INFO)
    _logger().info("Starting match data deletion process")
    cli, storage = await deps.get_storage()
    try:
        specs = {s.data_product: s for s in data_product_specs.get_data_products()}
        async def proc(m):
            await _delete_match(storage, specs, m)
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
        pickleable_dependencies: PickleableDependencies,
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
        # add jobs without triggers that will run on startup
        self._schd.add_job(self._move_matches_to_deletion)
        self._schd.add_job(self._delete_matches)
        # add jobs with triggers that will run after interval_sec
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
