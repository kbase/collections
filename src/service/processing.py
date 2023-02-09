"""
Common code for processing tasks that may take extended time, e.g. matches and selections,
both performing the initial match and calculating secondary data products.
"""

import asyncio
import multiprocessing

from pydantic import BaseModel, Field
from typing import Callable, Any
from src.service.app_state import PickleableDependencies


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
