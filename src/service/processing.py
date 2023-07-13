"""
Common code for processing tasks that may take extended time, e.g. matches and selections,
both performing the initial match and calculating secondary data products.
"""

import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import logging
import multiprocessing

from pydantic import BaseModel, Field
from typing import Callable, Any, Awaitable
from src.service.app_state_data_structures import PickleableDependencies, CollectionsState
from src.service import models
from src.service.storage_arango import ArangoStorage
from src.service.timestamp import now_epoch_millis


HEARTBEAT_INTERVAL_SEC = 10  # make configurable?
HEARTBEAT_RESTART_THRESHOLD_MS = 60 * 1000  # 1 minute without a heartbeat


_KEY_DPID = "dpid"
_KEY_SUBSET_FN = "subset_fn"


class CollectionProcess(BaseModel):
    """
    Defines a long running async process.
    Generally speaking, the process might run long enough that we wouldn't want the server
    to block a request until the process complete.
    """
    process: Callable[[str, PickleableDependencies, list[Any]], None] = Field(
        description="An async callable to run in a separate process. Takes an ID for data "
            + "that defines what the callable should do (typically a match or selection ID),"
            + "the storage system, and the arguments for the process as the callable arguments."
    )
    data_id: str = Field(
        description="The ID of data used to define what the callable should do, typically an "
            + "internal match or selection ID."
    )
    args: list[Any] = Field(
        description="The arguments for the process."
    )

    def start(self, deps: PickleableDependencies):
        """
        Start the process in a forkserver.

        deps - the system dependencies, pickleable.
        """
        run_async_process(target=self.process, args=(self.data_id, deps, self.args))


def run_async_process(target: Callable, args: list[Any]):
    """
    Start a separate process, calliung `target` with the provided `args` and starting the event
    loop.
    """
    ctx = multiprocessing.get_context("forkserver")
    ctx.Process(target=_run_async_process, args=[target, args]).start()


def _run_async_process(target: Callable, args: list[Any]):
    asyncio.run(target(*args))


def requires_restart(current_time_epoch_ms: int, process: models.ProcessAttributes) -> bool:
    f"""
    Check if a process should be restarted.

    current_time_epoch_ms - the current time in milliseconds since the epoch.
    process - the process to check.

    Returns true if:
    * the state of the process is {models.ProcessState.PROCESSING.value} and
    * one of the following is true
        * the process has never sent a heartbeat and the process was created >
          {HEARTBEAT_RESTART_THRESHOLD_MS} ms ago
        * the heartbeat is > {HEARTBEAT_RESTART_THRESHOLD_MS} ms old
    """
    if process.state == models.ProcessState.PROCESSING:
        # "failed" indicates the failure is not necessarily recoverable
        # E.g. an admin should take a look
        # We may need to add another state for recoverable errors like loss of contact w/ arango...
        # but that kind of thing could lead to a lot of jobs being kicked off over and over
        # Better to put retries in the matching code or arango storage wrapper
        maxdiff = HEARTBEAT_RESTART_THRESHOLD_MS
        if process.heartbeat is None:
            if current_time_epoch_ms - process.created > maxdiff:
                return True
        elif current_time_epoch_ms - process.heartbeat > maxdiff:
            return True
    return False


class Heartbeat:
    """
    Sends a heatbeat timestamp in epoch seconds to an async function at a specified interval,
    running in the default asyncio event loop.
    """

    def __init__(self, heartbeat_function: Callable[[int], None], interval_sec: int):
        """
        Create the hearbeat instance.

        heartbeat_function - the async function to call on each heartbeat. Accepts a single
            argument which is the heartbeat timestamp in milliseconds since the Unix epoch.
        interval_sec - the interval between heartbeats in seconds.
        """
        self._hbf = heartbeat_function
        self._interval_sec = interval_sec
        self._schd = AsyncIOScheduler()
        self._job_id = None

    def start(self):
        """
        Start the heartbeat, sending the first heartbeat after one interval.
        """
        if self._job_id:
            raise ValueError("Heartbeat is already running")
        self._schd.add_job(self._heartbeat)  # run immediately
        job = self._schd.add_job(self._heartbeat, "interval", seconds=self._interval_sec)
        self._job_id = job.id
        self._schd.start()

    async def _heartbeat(self):
        await self._hbf(now_epoch_millis())

    def stop(self):
        """
        Stop the heartbeat.
        """
        if self._job_id:
            self._schd.shutdown(wait=True)
            self._schd.remove_job(self._job_id)
            self._job_id = None


async def get_or_create_data_product_process(
    appstate: CollectionsState,
    dpid: models.DataProductProcessIdentifier,
    subset_fn: Callable[
        [
            PickleableDependencies,
            ArangoStorage,
            models.InternalMatch | models.InternalSelection,
            models.SavedCollection,
            models.DataProductProcessIdentifier
        ],
        Awaitable[None],
    ],
) -> models.DataProductProcess:
    """
    Get a process data structure for a data product process.

    Creates and starts the process if the process does not already exist.

    If the process exists but hasn't had a heartbeat in the required time frame, starts another
    instance of the process.

    appdate - the collections service state.
    dpid - the identifier for the process to create.
    subset_fn - the callable to run if there is no process information in the database or
        the heartbeat is too old. The arguments are
            * the system dependencies (provided by `appstate`),
            * a storage system created from the system dependencies. The storage system will
              be closed by the calling function after the callable returns
            * the match or selection
            * the collection (pulled from the storage system via the data in the subset record)
            * the data product process identifier.
    """
    args = [{_KEY_DPID: dpid, _KEY_SUBSET_FN: subset_fn}]
    now = appstate.get_epoch_ms()
    dp_proc, exists = await appstate.arangostorage.create_or_get_data_product_process(
        models.DataProductProcess(
            data_product=dpid.data_product,
            type=dpid.type,
            internal_id=dpid.internal_id,
            created=now,
            state=models.ProcessState.PROCESSING,
            state_updated=now,
        )
    )
    if not exists:
        _start_process(
            dpid.internal_id, _process_subset, appstate.get_pickleable_dependencies(), args)
    elif requires_restart(appstate.get_epoch_ms(), dp_proc):
        logging.getLogger(__name__).warn(
            f"Restarting {dpid.type.value} process for internal ID {dpid.internal_id} "
            + f"data product {dpid.data_product}"
        )
        _start_process(
            dpid.internal_id, _process_subset, appstate.get_pickleable_dependencies(), args)
    return dp_proc


def _start_process(
    internal_id: str,
    process_callable: Callable[[str, PickleableDependencies, list[Any]], None],
    deps: PickleableDependencies,
    args: list[Any],
) -> None:
    CollectionProcess(process=process_callable, data_id=internal_id, args=args).start(deps)


async def _process_subset(
    internal_id: str,
    deps: PickleableDependencies,
    args: list[dict[str,
        models.DataProductProcessIdentifier |
        Callable[
            [
                PickleableDependencies,
                ArangoStorage,
                models.InternalMatch | models.InternalSelection,
                models.SavedCollection,
                models.DataProductProcessIdentifier
            ],
            Awaitable[None],
        ]
    ]],
):
    # TODO DOCS document that processes should run regardless of the state
    #      of any other processes for the same match or selection. It is up to the code starting
    #      the process to ensure it is correct to start. As such, the processes should be
    #      idempotent.
    dpid = args[0][_KEY_DPID]
    subset_fn = args[0][_KEY_SUBSET_FN]
    hb = None
    arangoclient = None
    try:
        arangoclient, storage = await deps.get_storage()
        if dpid.type == models.SubsetType.MATCH:
            collspec = await storage.get_match_by_internal_id(dpid.internal_id)
        else:
            collspec = await storage.get_selection_by_internal_id(dpid.internal_id)
        async def heartbeat(millis: int):
            await storage.send_data_product_heartbeat(dpid, millis)
        hb = Heartbeat(heartbeat, HEARTBEAT_INTERVAL_SEC)
        hb.start()
        
        # use version number to avoid race conditions with activating collections
        coll = await storage.get_collection_version_by_num(
            collspec.collection_id, collspec.collection_ver
        )
        await subset_fn(deps, storage, collspec, coll, dpid)
    except Exception as _:
        logging.getLogger(__name__).exception(
            f"{dpid.type.value} process {dpid.internal_id} for data product "
            + f"{dpid.data_product} failed")
        await storage.update_data_product_process_state(
            dpid, models.ProcessState.FAILED, deps.get_epoch_ms())
    finally:
        if hb:
            hb.stop()
        if arangoclient:
            await arangoclient.close()
