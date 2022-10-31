"""
Functions for creating and handling application state.
"""

import aioarango
import asyncio
import sys

from fastapi import FastAPI, Request
from src.service.config import CollectionsServiceConfig
from src.service.kb_auth import KBaseAuth
from src.service.storage_arango import ArangoStorage, ARANGO_ERR_NAME_EXISTS

# The main point of this module is to handle all the stuff we add to app.state in one place
# to keep it consistent and allow for refactoring without breaking other code

async def build_app(app: FastAPI, cfg: CollectionsServiceConfig) -> None:
    """ Build the application state. """
    app.state._kb_auth = await KBaseAuth.create(cfg.auth_url, cfg.auth_full_admin_roles)
    cli, storage = await _build_storage(cfg)
    app.state._arango_cli = cli
    app.state._storage = storage


async def clean_app(app: FastAPI) -> None:
    """
    Clean up the application state, shutting down external connections and releasing resources.
    """
    if app.state._arango_cli:
        await app.state._arango_cli.close()


async def _build_storage(cfg: CollectionsServiceConfig
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
            db, create_collections_on_startup=cfg.create_db_on_startup)
        return cli, storage
    except:
        await cli.close()
        raise


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