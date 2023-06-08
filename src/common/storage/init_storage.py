"""
Initialize the ArangoDB database with sharded collections.
"""

import src.common.storage.collection_and_field_names as names
import typing


def _get_collections():
    colls = {}
    hints = typing.get_type_hints(names, include_extras=True)
    for field in hints:
        if hints[field].__metadata__[0] == names.COLL_ANNOTATION:
            collname = getattr(names, field)
            colls[collname] = hints[field].__metadata__[1]
    return colls


def main():
    colls = _get_collections()
    print("Default ArangoDB collection sharding values:")
    for i, coll in enumerate(colls):
        print(f"[{i + 1:03}] Collection name: {coll}")
        print(f"      Description: {colls[coll][names.COLL_ANNOKEY_DESCRIPTION]}")
        print(f"      Shards: {colls[coll][names.COLL_ANNOKEY_SUGGESTED_SHARDS]}")
    print("Set up collections (y) or edit (n)? y/[n]")  # TODO find a lib for doing this
    # TODO INITCLI - set sharding for all at the same time
    # TODO INITCLI - ask one by one
    # TODO INITCLI - needs deploy.cfg file for db creds


if __name__ == "__main__":
    main()
