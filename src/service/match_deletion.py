"""
Routines to safely delete matches and clean up match data in the collections system
"""

from apscheduler.schedulers.background import BackgroundScheduler

from datetime import datetime

from src.service import app_state
from src.service import processing

async def _move_matches_to_deletion(deps: app_state.PickleableDependencies, match_age_ms: int):
    print("_move_matches_to_deletion", datetime.now(), match_age_ms, flush=True)
    # Find matches with last_access < now - age
    # Save deleted match
    # Delete std match IF last_access hasn't changed
    #   If it has, punt and let _delete_matches clean up (or a future run of _move)


async def _delete_matches(deps: app_state.PickleableDependencies):
    print("_delete_matches", datetime.now(), flush=True)
    # * Find deleted matches
    # * If standard match exists (e.g. server went down)
    #   * if last_access is the same as the deleted match, delete the standard match
    #     (requiring last_access to be the same when deleting) and continue
    #     * if last_access changes during that time punt
    #   * otherwise, delete the deleted match if last_access is the same as previous and punt
    #     either way
    # * For each data product in the collection
    #   * remove all secondary data for each DP
    #   * remove the DP match doc
    # * Remove the deleted match doc


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
