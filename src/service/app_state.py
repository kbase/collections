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
from src.service.kb_auth import KBaseAuth
from src.service.storage_arango import ArangoStorage, ARANGO_ERR_NAME_EXISTS

# The main point of this module is to handle all the stuff we add to app.state in one place
# to keep it consistent and allow for refactoring without breaking other code


async def build_app(
    app: FastAPI, cfg: CollectionsServiceConfig, data_products: list[DataProductSpec]
) -> None:
    """ Build the application state. """
    app.state._kb_auth = await KBaseAuth.create(cfg.auth_url, cfg.auth_full_admin_roles)
    # pickling problems with the full spec, see
    # https://github.com/cloudpipe/cloudpickle/issues/408
    app.state._data_products = {dp.data_product: dp.db_collections for dp in data_products}
    cli, storage = await _build_storage(cfg, app.state._data_products)
    app.state._cfg = cfg
    app.state._arango_cli = cli
    app.state._storage = storage
    app.state._ws_url = await _get_workspace_url(cfg)
    # allow generating the workspace client to be mocked out in a request mock
    app.state._get_ws = lambda token: Workspace(app.state._ws_url, token=token)


async def clean_app(app: FastAPI) -> None:
    """
    Clean up the application state, shutting down external connections and releasing resources.
    """
    if app.state._arango_cli:
        await app.state._arango_cli.close()


async def _build_storage(
    cfg: CollectionsServiceConfig,
    data_products: dict[str, list[DBCollection]],
) -> tuple[aioarango.ArangoClient, ArangoStorage]:
    if cfg.dont_connect_to_external_services:
        return False, False
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


async def _get_workspace_url(cfg: CollectionsServiceConfig) -> str:
    if cfg.dont_connect_to_external_services:
        return False
    try:
        ws = Workspace(cfg.workspace_url)
        # could check the version later if we add dependencies on newer versions
        print("Workspace version: " + ws.ver())
    except Exception as e:
        raise ValueError(f"Could not connect to workspace at {cfg.workspace_url}: {str(e)}") from e
    return cfg.workspace_url


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


def get_kbase_auth(r: Request) -> KBaseAuth:
    """ Get the KBase authentication instance for the application. """
    return r.app.state._kb_auth


def get_storage(r: Request) -> ArangoStorage:
    """ Get the storage instance for the application. """
    if not r.app.state._storage:
        raise ValueError("Service is running in databaseless mode")
    return r.app.state._storage


def get_workspace(r: Request, token: str) -> Workspace:
    """
    Get a workspace client initialized for a user.

    r - the incoming service request.
    token - the user's token.
    """
    if not r.app.state._ws_url:
        raise ValueError("Service is running in no external connections mode")
    return r.app.state._get_ws(token)


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


def get_pickleable_dependencies(r: Request) -> PickleableDependencies:
    """
    Get an object that can be pickled, passed to another process, and used to reinitialize the
    system dependencies there.
    """
    return PickleableDependencies(r.app.state._cfg, r.app.state._data_products)
