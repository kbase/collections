"""
Manages the service dynamic configuration.

For now, this just means fetching it from the db when requested and caching it for a specified
amount of time. It's expected that changes to the config are made by manually editing the
database.

In the future, it may handle edits as well, with help from the arango storage wrapper.
"""

from src.service.storage_arango import ArangoStorage
import time
from typing import Callable
from src.service.models import DynamicConfig


class DynamicConfigManager:
    """ A manager for the Collections service dynamic configuration. """
    
    def __init__(
        self,
        db: ArangoStorage,
        cache_for_sec: int=30,
        timestamp_fn: Callable[[], float] = time.time
    ):
        """
        Create the manager.
        
        db - the storage system containing the config.
        cache_for_sec - how long to cache the config before making another DB request.
        timestamp_fn - a function that returns the current seconds since the Unix epoch. Used for
           testing; normally should be left as the default
        """
        self._db = db
        self._cache_for_sec = cache_for_sec
        self._config = None
        # I think setting the last update to prior to the big bang is probably safe
        self._last = -14000000000 * 365 * 24 * 60 * 60
        self._timestamp = timestamp_fn
    
    async def get_config(self) -> DynamicConfig:
        """ Get the service config """
        ts = self._timestamp()
        if ts > self._last + self._cache_for_sec:
            self._config = await self._db.get_dynamic_config()
            self._last = ts
        return self._config
