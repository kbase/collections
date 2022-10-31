"""
A storage system for collections based on an Arango backend.
"""

import hashlib

from aioarango.database import StandardDatabase
from aioarango.exceptions import CollectionCreateError, DocumentInsertError
from src.service import models
from src.service import errors


# service collection names that aren't shared with data loaders.
_COLL_PREFIX = "kbcoll_coll_"
_COLL_COUNTERS = _COLL_PREFIX + "counters"
_COLL_VERSIONS = _COLL_PREFIX + "versions"
_COLL_ACTIVE = _COLL_PREFIX + "active"

_FLD_KEY = "_key"
_FLD_COLLECTION = "collection"
_FLD_COLLECTION_ID = "collection_id"
_FLD_COUNTER = "counter"

_ARANGO_SPECIAL_KEYS = [_FLD_KEY, "_id", "_rev"]
ARANGO_ERR_NAME_EXISTS = 1207
_ARANGO_ERR_UNIQUE_CONSTRAINT = 1210

_QUERY_GET_NEXT_VERSION = f"""
    UPSERT {{{_FLD_KEY}: @{_FLD_COLLECTION_ID}}}
        INSERT {{{_FLD_KEY}: @{_FLD_COLLECTION_ID}, {_FLD_COUNTER}: 1}}
        UPDATE {{{_FLD_COUNTER}: OLD.{_FLD_COUNTER} + 1}}
        IN @@{_FLD_COLLECTION}
        OPTIONS {{exclusive: true}}
        RETURN NEW
"""

# Will probably want alternate sorts in the future, will need indexes etc.
# Cross bridge etc.
_QUERY_LIST_COLLECTIONS = f"""
    FOR d in @@{_FLD_COLLECTION}
        SORT d.{_FLD_KEY} ASC
        RETURN d
"""

# TODO SCHEMA may want a schema checker at some point... YAGNI for now. See sample service

# Assume here that we're never going to make an alternate implementation of this interface.
# Seems incredibly unlikely


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
        if e.error_code != ARANGO_ERR_NAME_EXISTS:
            raise  # if collection exists, ignore, otherwise raise


def _md5(contents: str):
    return hashlib.md5(contents.encode('utf-8')).hexdigest()


def _version_key(collection_id: str, ver_tag: str):
    return _md5(f"{collection_id}_{ver_tag}")


# modifies doc in place
def _remove_arango_keys(doc: dict):
    for k in _ARANGO_SPECIAL_KEYS:
        doc.pop(k, None)
    return doc


def _doc_to_active_coll(doc: dict):
    return models.ActiveCollection.construct(**_remove_arango_keys(doc))


class ArangoStorage:
    """
    An arango wrapper for collections storage.
    """

    @classmethod
    async def create(
        cls,
        db: StandardDatabase,
        create_collections_on_startup: bool = False
    ):
        """
        Create the ArangoDB wrapper.

        db - the database where the data is stored. The DB must exist.
        create_collections_on_startup - on starting the wrapper, create all collections and indexes
            rather than just checking for their existence. Usually this should be false to
            allow for system administrators to set up sharding to their liking, but auto
            creation is useful for quickly standing up a test service.
        """
        if create_collections_on_startup:
            await _create_collection(db, _COLL_COUNTERS)  # no indexes necessary
            await _create_collection(db, _COLL_ACTIVE)  # no indexes necessary yet
            await _create_collection(db, _COLL_VERSIONS)
        else:
            await _check_collection_exists(db, _COLL_COUNTERS)
            await _check_collection_exists(db, _COLL_ACTIVE)
            await _check_collection_exists(db, _COLL_VERSIONS)
        vercol = db.collection(_COLL_VERSIONS)
        await vercol.add_persistent_index([models.FIELD_COLLECTION_ID, models.FIELD_VER_NUM])
        return ArangoStorage(db)

    def __init__(self, db: StandardDatabase):
        self._db = db
        # TODO DB make some sort of pluginish system for data products

    async def get_next_version(self, collection_id: str) -> int:
        """ Get the next available version number for a collection. """
        bind_vars = {
            _FLD_COLLECTION_ID: collection_id,
            f"@{_FLD_COLLECTION}": _COLL_COUNTERS
        }
        cur = await self._db.aql.execute(_QUERY_GET_NEXT_VERSION, bind_vars=bind_vars)
        verdoc = await cur.next()
        return verdoc[_FLD_COUNTER]

    async def save_collection_version(self, collection: models.SavedCollection) -> None:
        """
        Save a version of a collection. The version is not active.
        The caller is expected to use the `get_next_version` method to get a version number for
        the collection.
        """
        doc = collection.dict()
        # ver_tag is pretty unconstrained so MD5 to get rid of any weird characters
        doc[_FLD_KEY] = _version_key(collection.id, collection.ver_tag)
        col = self._db.collection(_COLL_VERSIONS)
        try:
            await col.insert(doc)
        except DocumentInsertError as e:
            if e.error_code == _ARANGO_ERR_UNIQUE_CONSTRAINT:
                raise errors.CollectionVersionExistsError(
                    f"There is already a collection {collection.id} "
                    + f"with version {collection.ver_tag}")
            else:
                raise e

    async def save_collection_active(self, collection: models.ActiveCollection) -> None:
        """
        Save a collection, making it active.
        The caller is expected to retrive a collection from a `get_collection_version_by_*`
        method, update it to an active collection, and save it here.
        """
        doc = collection.dict()
        doc[_FLD_KEY] = collection.id
        col = self._db.collection(_COLL_ACTIVE)
        await col.insert(doc, overwrite=True)

    async def get_collections_active(self) -> list[models.ActiveCollection]:
        bind_vars = {
            f"@{_FLD_COLLECTION}": _COLL_ACTIVE
        }
        cur = await self._db.aql.execute(_QUERY_LIST_COLLECTIONS, bind_vars=bind_vars)
        return [_doc_to_active_coll(d) async for d in cur]

    async def get_collection_active(self, collection_id: str) -> models.ActiveCollection:
        """ Get an active collection. """
        col = self._db.collection(_COLL_ACTIVE)
        doc = await col.get(collection_id)
        if doc is None:
            raise errors.NoSuchCollectionError(
                f"There is no active collection {collection_id}")
        return _doc_to_active_coll(doc)

    async def has_collection_version_by_tag(self, collection_id: str, ver_tag: str) -> bool:
        """ Check if a collection version exists. """
        col = self._db.collection(_COLL_VERSIONS)
        return await col.has(_version_key(collection_id, ver_tag))

    async def get_collection_version_by_tag(self, collection_id: str, ver_tag: str
    ) -> models.SavedCollection:
        """ Get a collection version by its version tag. """
        col = self._db.collection(_COLL_VERSIONS)
        doc = await col.get(_version_key(collection_id, ver_tag))
        if doc is None:
            raise errors.NoSuchCollectionVersionError(
                f"No collection {collection_id} with version tag {ver_tag}")
        # if we really want to be careful here we could check that the id and tag match
        # the returned doc, since theoretically it might be possible to construct an id and tag
        # that collide with the md5 of another id and tag. YAGNI for now.
        return models.SavedCollection.construct(**_remove_arango_keys(doc))

    async def get_collection_version_by_num(self, collection_id: str, ver_num: int
    ) -> models.SavedCollection:
        """ Get a collection version by its version number. """
        col = self._db.collection(_COLL_VERSIONS)
        cur = await col.find({
            models.FIELD_COLLECTION_ID: collection_id,
            models.FIELD_VER_NUM: ver_num
        })
        if cur.count() > 1:
            raise ValueError(
                f"Found more than 1 document in the db for collection {collection_id} "
                + f"and version number {ver_num}")
        if cur.count() < 1:
            raise errors.NoSuchCollectionVersionError(
                f"No collection {collection_id} with version number {ver_num}")
        doc = await cur.next()
        return models.SavedCollection.construct(**_remove_arango_keys(doc))
