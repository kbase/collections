"""
Functions for creating and handling application state.

All functions assume that the application state has been appropriately initialized via
calling the build_app() method
"""

import aioarango
import asyncio
import sys

from fastapi import FastAPI, Request
from src.service.clients.workspace_client import Workspace
from src.service.config import CollectionsServiceConfig
from src.service.data_products.common_models import DataProductSpec, DBCollection
from src.service.matchers.common_models import Matcher
from src.service.kb_auth import KBaseAuth
from src.service.storage_arango import ArangoStorage, ARANGO_ERR_NAME_EXISTS
from src.service.timestamp import now_epoch_millis

# The main point of this module is to handle all the application state in one place
# to keep it consistent and allow for refactoring without breaking other code


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
        self._dps = data_products
    
    async def get_storage(self) -> tuple[aioarango.ArangoClient, ArangoStorage]:
        """
        Get the Arango client and storage system. The arango client must be closed when the
        storage system is no longer necessary.
        """
        return await _build_storage(self._cfg, self._dps)

    async def get_workspace(self, token):
        """
        Get the workspace client.

        token - the user's token.
        """
        return Workspace(cfg.workspace_url, token=token)
    
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
    """

    def __init__(
        self,
        auth: KBaseAuth,
        arangoclient: aioarango.ArangoClient,
        arangostorage: ArangoStorage,
        data_products: dict[str, list[DBCollection]],
        matchers: list[Matcher],
        cfg: CollectionsServiceConfig,
    ):
        """
        Do not instantiate this class directly. Use `build_app` to create the app state and
        `get_app_state` or `get_app_state_from_app` to access it.
        """
        self.auth = auth
        self._client = arangoclient
        self.arangostorage = arangostorage
        self._data_products = data_products
        self._matchers = {m.id: m for m in matchers}
        self._cfg = cfg

    async def destroy(self):
        """
        Destroy any resources held by this class. After this the class should be discarded.
        """
        await self._client.close()

    def get_workspace_client(self, token) -> Workspace:
        """
        Get a client for the KBase Workspace.

        token - the user's token.
        """
        return Workspace(self._cfg.workspace_url, token=token)

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


async def build_app(
    app: FastAPI,
    cfg: CollectionsServiceConfig,
    data_products: list[DataProductSpec],
    matchers: list[Matcher],
) -> None:
    """
    Build the application state.

    app - the FastAPI app.
    cfg - the collections service config.
    data_products - the data products installed in the system
    matchers - the matchers installed in the system
    """
    auth = await KBaseAuth.create(cfg.auth_url, cfg.auth_full_admin_roles)
    # pickling problems with the full spec, see
    # https://github.com/cloudpipe/cloudpickle/issues/408
    data_products = {dp.data_product: dp.db_collections for dp in data_products}
    await _check_workspace_url(cfg)
    # do this last in case the steps above throw an exception. cli needs to be closed
    cli, storage = await _build_storage(cfg, data_products)
    app.state._colstate = CollectionsState(auth, cli, storage, data_products, matchers, cfg)


def get_app_state(r: Request) -> CollectionsState:
    """
    Get the application state from a request.
    """
    return get_app_state_from_app(r.app)


def get_app_state_from_app(app: FastAPI) -> CollectionsState:
    """
    Get the application state given a FastAPI app.
    """
    if not app.state._colstate:
        raise ValueError("App state has not been initialized")
    return app.state._colstate


async def _build_storage(
    cfg: CollectionsServiceConfig,
    data_products: dict[str, list[DBCollection]],
) -> tuple[aioarango.ArangoClient, ArangoStorage]:
    cli = aioarango.ArangoClient(hosts=cfg.arango_url)
    try:
        if cfg.create_db_on_startup:
            sysdb = await _get_arango_db(cli, "_system", cfg)
            try:
                await sysdb.create_database(cfg.arango_db)
            except aioarango.exceptions.DatabaseCreateError as e:
                if e.error_code != ARANGO_ERR_NAME_EXISTS:  # ignore, db exists
                    raise
        db = await _get_arango_db(cli, cfg.arango_db, cfg)
        storage = await ArangoStorage.create(
            db,
            data_products=data_products,
            create_collections_on_startup=cfg.create_db_on_startup
        )
        return cli, storage
    except:
        await cli.close()
        raise


async def _check_workspace_url(cfg: CollectionsServiceConfig) -> str:
    try:
        ws = Workspace(cfg.workspace_url)
        # could check the version later if we add dependencies on newer versions
        print("Workspace version: " + ws.ver())
    except Exception as e:
        raise ValueError(f"Could not connect to workspace at {cfg.workspace_url}: {str(e)}") from e


async def _get_arango_db(cli: aioarango.ArangoClient, db: str, cfg: CollectionsServiceConfig
) -> aioarango.database.StandardDatabase:
    err = None
    for t in [1, 2, 5, 10, 30]:
        try:
            if cfg.arango_user:
                rdb = await cli.db(
                    db, verify=True, username=cfg.arango_user, password=cfg.arango_pwd)
            else:
                rdb = await cli.db(db, verify=True)
            return rdb
        except aioarango.exceptions.ServerConnectionError as e:
            err = e
            print(  f"Failed to connect to Arango database at {cfg.arango_url}\n"
                  + f"    Error: {err}\n"
                  + f"    Waiting for {t}s and retrying db connection"
            )
            sys.stdout.flush()
            # TODO CODE both time.sleep() and this block SIGINT. How to fix?
            await asyncio.sleep(t)
    raise ValueError(f"Could not connect to Arango at {cfg.arango_url}") from err


