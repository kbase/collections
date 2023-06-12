"""
Initialize the ArangoDB database with sharded collections.
"""

import argparse
import aioarango
import asyncio
from src.service.config import CollectionsServiceConfig
from src.service._app_state_build_storage import build_arango_db
import src.common.storage.collection_and_field_names as names
from typing import get_type_hints, Any


_CONF_SHARDS = "confshards"
_COLL_NAME = "collname"


def _get_config() -> CollectionsServiceConfig:
    parser = argparse.ArgumentParser(
        description="Set up ArangoDB collection sharding for the KBase collectins service."
    )
    parser.add_argument(
        '-c', '--config', required=True, type=str,
        help="The path to a filled in collections_config.toml file as would be "
            + "provided to the service. The ArangoDB connection parameters are read from "
            + "this file."
    )
    args = parser.parse_args()
    with open(args.config, 'rb') as cfgfile:
        return CollectionsServiceConfig(cfgfile)
    

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
    print("Current ArangoDB collection sharding values:")
    for i, coll in enumerate(collections.values()):
        _print_collection_config(i, coll)


async def _user_loop(db: aioarango.database.StandardDatabase, colls: dict[str, dict[str, Any]]):
    print("Set up collections (y) or edit (n)? y/[n]")  # TODO find a lib for doing this
    # TODO INITCLI - set sharding for all at the same time
    # TODO INITCLI - ask one by one


async def main():
    config = _get_config()
    colls = _get_collections()
    print("This program allows you to set up sharding values for the KBase collections service "
           + "and, when ready, create the collections in ArangoDB.")
    _print_collections(colls)
    print("Connecting to db... ", end="")
    cli, db = await build_arango_db(config, create_database=True)
    print("done")
    try:
        await _user_loop(colls, db)
    finally:
        await cli.close()


if __name__ == "__main__":
    asyncio.run(main())
