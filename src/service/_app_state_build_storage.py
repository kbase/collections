import aioarango
import asyncio
import sys

from src.service.config import CollectionsServiceConfig
from src.service.data_products.common_models import DataProductSpec
from src.service.storage_arango import ArangoStorage, ARANGO_ERR_NAME_EXISTS


async def build_arango_db(cfg: CollectionsServiceConfig, create_database: bool = False
) -> tuple[aioarango.ArangoClient, aioarango.database.StandardDatabase]:
    cli = aioarango.ArangoClient(hosts=cfg.arango_url)
    try:
        if create_database:
            sysdb = await _get_arango_db(cli, "_system", cfg)
            try:
                await sysdb.create_database(cfg.arango_db)
            except aioarango.exceptions.DatabaseCreateError as e:
                if e.error_code != ARANGO_ERR_NAME_EXISTS:  # ignore, db exists
                    raise
        db = await _get_arango_db(cli, cfg.arango_db, cfg)
        return cli, db
    except:
        await cli.close()
        raise


async def build_storage(
    cfg: CollectionsServiceConfig,
    data_products: list[DataProductSpec],
) -> tuple[aioarango.ArangoClient, ArangoStorage]:
    cli, db = await build_arango_db(cfg, cfg.create_db_on_startup)
    try:
        storage = await ArangoStorage.create(
            db,
            data_products=data_products,
            create_collections_on_startup=cfg.create_db_on_startup
        )
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
            print(
                f"Failed to connect to Arango {db} database at {cfg.arango_url}\n"
                + f"    Error: {err}\n"
                + f"    Waiting for {t}s and retrying db connection",
                flush=True
            )
            sys.stdout.flush()
            # TODO CODE both time.sleep() and this block SIGINT. How to fix?
            await asyncio.sleep(t)
    raise ValueError(f"Could not connect to Arango at {cfg.arango_url}") from err
