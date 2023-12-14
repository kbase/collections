"""
Initialize the ArangoDB database with sharded collections.
"""

import argparse
import aioarango
import asyncio
import click
import time

import src.common.storage.collection_and_field_names as names
from src.service.config import CollectionsServiceConfig
from src.service._app_state_build_storage import build_arango_db
from src.service.storage_arango import ARANGO_ERR_NAME_EXISTS, ArangoStorage, ViewExistsError
from typing import get_type_hints, Any
from src.service import data_product_specs
from src.common.collection_column_specs import load_specs
from src.service.filtering import analyzers, generic_view


REPLICATION = 3

_START_TEXT = f"""
In this program, we will first create any missing ArangoDB collections
required for the KBase Collections service and specify their sharding.

* Sharding cannot be altered after the ArangoDB collections are created.
* Replication is set to {REPLICATION} and other properties are left as
  their defaults.
* Other properties can be adjusted post creation via the database
  shell / UI etc.

Next, we'll check for any needed updates for ArangoSearch views and
create new views if necessary.
""".strip() + "\n"

_CONF_SHARDS = "confshards"
_COLL_NAME = "collname"


def _get_config() -> CollectionsServiceConfig:
    parser = argparse.ArgumentParser(
        description="Manage the KBase collections service, including setting up sharding for "
            + "new collections and creating ArangoSearch views."
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
    

def _get_required_collections() -> dict[str, dict[str, Any]]:
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
    print("Proposed ArangoDB collection sharding values:")
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


async def _user_loop_sharding(
        colls: dict[str, dict[str, Any]],
        db: aioarango.database.StandardDatabase
    ):
    _print_collections(colls)
    while True:
        if click.confirm("Commit this sharding configuration?"):
            print("Creating collections... ", end="")
            await _setup_db(colls, db)
            print("done.")
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
            print()
            _print_collections(colls)


async def _get_missing_collections(
        required_cols: dict[str, dict[str, Any]],
        db: aioarango.database.StandardDatabase
    ) -> dict[str, dict[str, Any]]:
    print("Checking for missing ArangoDB collections... ", end="")
    colls = await db.collections()
    print("done")
    missing = sorted(set(required_cols.keys()) - {c["name"] for c in colls})
    return {m: required_cols[m] for m in missing}


def _print_view_status(data_product, db_collection_name, views_matching_spec):
    print(f"View status for data product {data_product}:")
    print("    Arango collection name:")
    print(f"        {db_collection_name}")
    print("    Database views matching current specs: ")
    print(f"        {views_matching_spec}")


async def _create_view(
        store: ArangoStorage,
        data_product: str,
        db_collection_name: str,
        create_generic_view: bool = False
    ):
    if create_generic_view:
        print(f"Creating generic view for data product {data_product}...")
        view_spec = generic_view.create_generic_spec()
        # TODO: force creating/replacing existing generic view
    else:
        print(f"Loading view spec for data product {data_product}...")
        view_spec = load_specs.load_spec(data_product)
        print("Found view specs:")
        for v in sorted(view_spec.spec_files):
            print(f"   {v}")

    views = sorted(await store.get_search_views_from_spec(
        db_collection_name,
        view_spec,
        analyzers.get_analyzer,
        include_all_fields=create_generic_view))
    _print_view_status(data_product, db_collection_name, views)
    if views:
        print("    Setup ok.")
    elif click.confirm("No view present for current view specifications. Create now?"):
        view_name = click.prompt("Please provide a name for the new view")
        t0 = time.time()
        print("Creating view ... ", end="", flush=True)
        try:
            await store.create_search_view(
                view_name,
                db_collection_name,
                view_spec,
                analyzers.get_analyzer,
                include_all_fields=create_generic_view)
            print(f"done in {time.time() - t0:.2f} seconds.")
            return view_name
        except ViewExistsError:
            print(f"A view with name {view_name} already exists and does not match the spec.")
    print("The view name should be configured for data products in KBase Collections ")
    print("at the same time the load version with the data matching the view specs is ")
    print("configured.")


async def _update_views(store: ArangoStorage):
    # TODO DELETE_VIEWS helper method to delete views? Or could be in API
    print("Checking ArangoSearch view status")
    for dp in data_product_specs.get_data_products():
        for dbc in dp.db_collections:
            if dbc.view_required or dbc.generic_view_required:
                await _create_view(
                    store,
                    dp.data_product,
                    dbc.name,
                    create_generic_view=dbc.generic_view_required
                )


async def main():
    config, skip_db_creation = _get_config()
    colls = _get_required_collections()
    print(_START_TEXT)
    print("Connecting to db... ", end="")
    cli, db = await build_arango_db(config, create_database=not skip_db_creation)
    print("done")
    try:
        colls_to_create = await _get_missing_collections(colls, db)
        print(f"{len(colls_to_create)} / {len(colls)} need to be created.")
        if colls_to_create:
            await _user_loop_sharding(colls_to_create, db)
        else:
            print("Huzzah! Nothing to be done.")
        print()
        store = await ArangoStorage.create(db)
        await _update_views(store)
    finally:
        await cli.close()


if __name__ == "__main__":
    asyncio.run(main())
