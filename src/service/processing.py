"""
Common code for processing tasks that may take extended time, e.g. matches and selections,
both performing the initial match and calculating secondary data products.
"""

import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
import multiprocessing

from pydantic import BaseModel, Field
from typing import Callable, Any
from src.service.app_state import PickleableDependencies
from src.service.timestamp import now_epoch_millis


HEARTBEAT_INTERVAL_SEC = 10  # make configurable?
HEARTBEAT_RESTART_THRESHOLD_SEC = 60  # 1 minute without a heartbeat


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
    args: list[Any] = Field(
        description="The arguments for the process."
    )

    def start(self, data_id: str, storage: PickleableDependencies):
        """
        Start the process in a forkserver.

        data_id - the ID of data used to define what the callable should do, typically a match
            or selection ID.
        storage - the storage system containing the ID data and the data to match against.
        """
        ctx = multiprocessing.get_context("forkserver")
        ctx.Process(target=_run_process, args=(self.process, data_id, storage, self.args)).start()


def _run_process(
    function: Callable[[str, PickleableDependencies, list[Any]], None],
    data_id: str,
    pstorage: PickleableDependencies,
    args: list[Any]
):
    asyncio.run(function(data_id, pstorage, args))


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
        job = self._schd.add_job(
            func=self._heartbeat,
            trigger=IntervalTrigger(seconds=self._interval_sec)
        )
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
