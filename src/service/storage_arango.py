"""
A storage system for collections based on an Arango backend.
"""

from aioarango.database import StandardDatabase
from aioarango.exceptions import CollectionCreateError
from src.service.models import SavedCollection


# service collection names that aren't shared with data loaders.
_COLL_PREFIX = "kbcoll_coll_"
_COLL_COUNTERS = _COLL_PREFIX + "counters"
_COLL_VERSIONS = _COLL_PREFIX + "versions"
_COLL_ACTIVE = _COLL_PREFIX + "active"

_FLD_KEY = "_key"
_FLD_COLLECTION = "collection"
_FLD_COLLECTION_ID = "collection_id"
_FLD_COUNTER = "counter"

_QUERY_GET_NEXT_VERSION = f"""
    UPSERT {{{_FLD_KEY}: @{_FLD_COLLECTION_ID}}}
        INSERT {{{_FLD_KEY}: @{_FLD_COLLECTION_ID}, {_FLD_COUNTER}: 1}}
        UPDATE {{{_FLD_COUNTER}: OLD.{_FLD_COUNTER} + 1}}
        IN @@{_FLD_COLLECTION}
        OPTIONS {{exclusive: true}}
        RETURN NEW
"""

# TODO SCHEMA may want a schema checker at some point... YAGNI for now. See sample service

# Assume here that we're never going to make an alternate implementation of this interface.
# Seems incredibly unlikely

async def create_storage(
        db: StandardDatabase,
        create_collections_on_startup: bool = False):
    """
    Create the wrapper.

    db - the database where the data is stored. The DB must exist.
    create_collections_on_startup - on starting the wrapper, create all collections and indexes
        rather than just checking for their existence. Usually this should be false to
        allow for system administrators to set up sharding to their liking, but auto
        creation is useful for quickly standing up a test service.
    """
    if create_collections_on_startup:
        # TODO DB create indexes and collections
        await _create_collection(db, _COLL_COUNTERS)  # no indexes necessary
    else:
        await _check_collection_exists(db, _COLL_COUNTERS)
        # TODO DB check collections and indexes exist
    return ArangoStorage(db)

async def _check_collection_exists(db: StandardDatabase, col_name: str):
    if not await db.has_collection(col_name):
        raise ValueError(f"Collection {col_name} does not exist")

async def _create_collection(db: StandardDatabase, col_name: str):
    """
    Create a collection, ignoring a duplicate name error
    """
    try:
        await db.create_collection(col_name)
    except CollectionCreateError as e:
        if not e.error_code == 1207:  # duplicate name error code
            raise  # if collection exists, ignore, otherwise raise

class ArangoStorage:
    """
    An arango wrapper for collections storage.
    """

    def __init__(self, db: StandardDatabase):
        self._db = db
        # TODO DB make some sort of pluginish system for data products
        # TODO NOW document endpoint in new endpointsfile (in /docs/endpoint.md)

    async def get_next_version(self, collection_id: str) -> int:
        """ Get the next available version number for a collection. """
        bind_vars = {
            _FLD_COLLECTION_ID: collection_id,
            f"@{_FLD_COLLECTION}": _COLL_COUNTERS
        }
        cur = await self._db.aql.execute(_QUERY_GET_NEXT_VERSION, bind_vars=bind_vars)
        verdoc = await cur.next()
        return verdoc[_FLD_COUNTER]

    async def save_collection(self, collection: SavedCollection, active: bool = False) -> None:
        # check that active user & date are set if active = true
        pass

    async def get_collections_active(self) -> list[SavedCollection]:
        pass

    async def get_collection_active(self, collection_id: str) -> SavedCollection:
        pass

    async def get_collection_version_by_tag(self, collection_id: str, ver_tag: str):
        pass

    async def get_collection_version_by_num(self, collection_id: str, ver_num: int):
        pass
