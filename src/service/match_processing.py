"""
Common code for processing matches.
"""

import multiprocessing

from pydantic import BaseModel, Field
from typing import Callable, Any
from src.service.app_state import PickleableStorage
from src.service.storage_arango import ArangoStorage


class MatchProcess(BaseModel):
    """
    Defines a match process.
    """
    process: Callable[[str, PickleableStorage, list[Any]], None] = Field(
        description="A callable that processes a match. Takes the match ID, the storage system, "
            + "and the arguments for the match as the callable aruguments."
    )
    args: list[Any] = Field(
        description="The arguments for the match."
    )

    def start(self, match_id: str, storage: PickleableStorage):
        """
        Start the match process in a forkserver.

        match_id - the ID of the match.
        storage - the storage system containing the match and the data to match against.
        """
        ctx = multiprocessing.get_context("forkserver")
        ctx.Process(target=self.process, args=(match_id, storage, self.args)).start()