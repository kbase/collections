"""
Initialize the ArangoDB database with sharded collections.
"""

import argparse
import aioarango
import asyncio
import click
import src.common.storage.collection_and_field_names as names
from src.service.config import CollectionsServiceConfig
from src.service._app_state_build_storage import build_arango_db
from src.service.storage_arango import ARANGO_ERR_NAME_EXISTS
from typing import get_type_hints, Any


REPLICATION = 3

_CONF_SHARDS = "confshards"
_COLL_NAME = "collname"


def _get_config() -> CollectionsServiceConfig:
    parser = argparse.ArgumentParser(
        description="Set up ArangoDB collection sharding for the KBase collections service."
    )
    parser.add_argument(
        '-c', '--config', required=True, type=str,
        help="The path to a filled in collections_config.toml file as would be "
            + "provided to the service. The ArangoDB connection parameters are read from "
            + "this file."
    )
    parser.add_argument(
        "-s", "--skip-database-creation", action="store_true",
        help="Don't create the database. This is necessary if the credentials in the config "
            + "file don't have permissions for the _system database; however the target database "
            + "must already exist."
    )
    args = parser.parse_args()
    with open(args.config, 'rb') as cfgfile:
        cfg = CollectionsServiceConfig(cfgfile)
    return cfg, args.skip_database_creation
    

def _get_collections() -> dict[str, dict[str, Any]]:
    colls = {}
    hints = get_type_hints(names, include_extras=True)
    for field in hints:
        if hints[field].__metadata__[0] == names.COLL_ANNOTATION:
            collname = getattr(names, field)
            colls[collname] = dict(hints[field].__metadata__[1])
            colls[collname][_CONF_SHARDS] = colls[collname][names.COLL_ANNOKEY_SUGGESTED_SHARDS]
            colls[collname][_COLL_NAME] = collname
    return colls


def _print_collection_config(index: int, collection: dict[str, Any]):
    print(f"[{index + 1:03}] Collection name: {collection[_COLL_NAME]}")
    print(f"      Description: {collection[names.COLL_ANNOKEY_DESCRIPTION]}")
    print(f"      Shards suggested: {collection[names.COLL_ANNOKEY_SUGGESTED_SHARDS]} "
                  + f"To be created: {collection[_CONF_SHARDS]}"
    )


def _print_collections(collections: dict[str, dict[str, Any]]):
    print("\nCurrent ArangoDB collection sharding values:")
    for i, coll in enumerate(collections.values()):
        _print_collection_config(i, coll)
    print()


async def _setup_db(colls: dict[str, dict[str, Any]], db: aioarango.database.StandardDatabase):
    for coll in colls.values():
        try:
            await db.create_collection(
                coll[_COLL_NAME],
                shard_count=coll[_CONF_SHARDS],
                replication_factor=REPLICATION
            )
        except aioarango.exceptions.CollectionCreateError as e:
            if e.error_code != ARANGO_ERR_NAME_EXISTS:  # already exists, ignore
                raise
            else:
                print(f"Collection {coll[_COLL_NAME]} already exists, ignoring.")


async def _user_loop(colls: dict[str, dict[str, Any]], db: aioarango.database.StandardDatabase):
    while True:
        if click.confirm("Commit this sharding configuration?"):
            print("Creating collections... ")
            await _setup_db(colls, db)
            print("Done. Buh-bye!")
            return
        else:
            index = -1
            while index < 1:
                index = click.prompt("Which collection would you like to edit?", type=int)
                if index < 1 or index > len(colls):
                    print(f"Please enter a number between 1 and {len(colls)}.")
                    index = -1
            index = index - 1
            coll = list(colls.values())[index]
            _print_collection_config(index, coll)
            shards = -1
            while shards < 1:
                shards = click.prompt("How many shards would you like?", type=int)
                if shards < 1:
                    print("Please enter a number greater than 0.")
            coll[_CONF_SHARDS] = shards
            _print_collections(colls)


async def main():
    config, skip_db_creation = _get_config()
    colls = _get_collections()
    print("This program allows you to set up sharding values for the KBase collections service "
           + "and, when ready, create the collections in ArangoDB.")
    print("Only the sharding is set up because other factors can be adjusted post creation.")
    print(f"Replication is set to {REPLICATION} and other properties are left as their defaults.")
    _print_collections(colls)
    print("Connecting to db... ", end="")
    cli, db = await build_arango_db(config, create_database=not skip_db_creation)
    print("done")
    try:
        await _user_loop(colls, db)
    finally:
        await cli.close()


if __name__ == "__main__":
    asyncio.run(main())
