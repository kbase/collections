"""
Data structures to store app state.
"""

import aioarango

from src.service._app_state_build_storage import build_storage
from src.service.config import CollectionsServiceConfig
from src.service.data_products.common_models import DBCollection
from src.service.kb_auth import KBaseAuth
from src.service.matchers.common_models import Matcher
from src.service.sdk_async_client import SDKAsyncClient
from src.service.storage_arango import ArangoStorage
from src.service.timestamp import now_epoch_millis


class PickleableDependencies:
    """
    Enables getting a system dependencies in a separate process via pickling the information needed
    to recreate said dependencies.
    """

    def __init__(
        self,
        cfg: CollectionsServiceConfig,
        data_products: dict[str, list[DBCollection]]
    ):
        self._cfg = cfg
        # TODO CODE do we need to pickle this anymore? Just get from the registry
        self._dps = data_products
    
    async def get_storage(self) -> tuple[aioarango.ArangoClient, ArangoStorage]:
        """
        Get the Arango client and storage system. The arango client must be closed when the
        storage system is no longer necessary.
        """
        return await build_storage(self._cfg, self._dps)

    def get_epoch_ms(self) -> int:
        """
        Get the Unix epoch time in milliseconds.
        """
        # This allows for easy mocking of time generation rather than having to monkey patch
        # time.time
        return now_epoch_millis()


class CollectionsState:
    """
    State information about the collections system. Contains means to access DB storage,
    external systems, etc.

    Instance variables:

    auth - a KBaseAuth client.
    arangostorage - an ArangoStorage wrapper.
    sdk_client - a client for communicating with KBase SDK services.
    """

    def __init__(
        self,
        auth: KBaseAuth,
        sdk_client: SDKAsyncClient,
        arangoclient: aioarango.ArangoClient,
        arangostorage: ArangoStorage,
        data_products: dict[str, list[DBCollection]],
        matchers: list[Matcher],
        cfg: CollectionsServiceConfig,
    ):
        """
        Do not instantiate this class directly. Use `app_state.build_app` to create the app state
        and `app_state.get_app_state` or `app_state.get_app_state_from_app` to access it.
        """
        self.auth = auth
        self._client = arangoclient
        self.sdk_client = sdk_client
        self.arangostorage = arangostorage
        self._data_products = data_products
        self._matchers = {m.id: m for m in matchers}
        self._cfg = cfg

    async def destroy(self):
        """
        Destroy any resources held by this class. After this the class should be discarded.
        """
        await self._client.close()
        await self.sdk_client.close()

    def get_pickleable_dependencies(self) -> PickleableDependencies:
        """
        Get an object that can be pickled, passed to another process, and used to reinitialize the
        system dependencies there.
        """
        return PickleableDependencies(self._cfg, self._data_products)

    def get_matcher(self, matcher_id) -> Matcher | None:
        """
        Get a matcher by its ID. Returns None if no such matcher exists.
        """
        return self._matchers.get(matcher_id)

    def get_matchers(self) -> list[Matcher]:
        """
        Get all the matchers registered in the system.
        """
        return list(self._matchers.values())

    def get_epoch_ms(self) -> int:
        """
        Get the Unix epoch time in milliseconds.
        """
        # This allows for easy mocking of time generation rather than having to monkey patch
        # time.time
        return now_epoch_millis()
