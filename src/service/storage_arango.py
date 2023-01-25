"""
A storage system for collections based on an Arango backend.
"""

from aioarango.aql import AQL
from aioarango.database import StandardDatabase
from aioarango.exceptions import CollectionCreateError, DocumentInsertError
from fastapi.encoders import jsonable_encoder
from typing import Any
from src.common.hash import md5_string
from src.common.storage.collection_and_field_names import (
    FLD_ARANGO_KEY,
    FLD_ARANGO_ID,
    COLL_SRV_ACTIVE,
    COLL_SRV_COUNTERS,
    COLL_SRV_VERSIONS,
    COLL_SRV_MATCHES,
)
from src.service import models
from src.service import errors
from src.service.data_products.common_models import DataProductSpec


# service collection names that aren't shared with data loaders.

_FLD_COLLECTION = "collection"
_FLD_COLLECTION_ID = "collection_id"
_FLD_MATCH_ID = "match_id"
_FLD_COUNTER = "counter"
_FLD_VER_NUM = "ver_num"
_FLD_LIMIT = "limit"

_ARANGO_SPECIAL_KEYS = [FLD_ARANGO_KEY, FLD_ARANGO_ID, "_rev"]
ARANGO_ERR_NAME_EXISTS = 1207
_ARANGO_ERR_UNIQUE_CONSTRAINT = 1210

_COLLECTIONS = [COLL_SRV_ACTIVE, COLL_SRV_COUNTERS, COLL_SRV_VERSIONS, COLL_SRV_MATCHES]
_BUILTIN = "builtin"

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


def _version_key(collection_id: str, ver_tag: str):
    return md5_string(f"{collection_id}_{ver_tag}")


def remove_arango_keys(doc: dict):
    """ Removes the 3 special arango keys from a document **in place**. """
    for k in _ARANGO_SPECIAL_KEYS:
        doc.pop(k, None)
    return doc


_DP = models.FIELD_DATA_PRODUCTS
_MTC = models.FIELD_MATCHERS


def _matcher_docs_to_model(docs: list[dict[str, Any]]):
    if not docs:
        # backwards compatibility for *really* old code. Kind of want to remove it but doesn't
        # really hurt either
        return []
    return [models.Matcher.construct(**m) for m in docs]

def _data_product_docs_to_model(docs: list[dict[str, str]]):
    return [models.DataProduct.construct(**dp) for dp in docs]


def _doc_to_active_coll(doc: dict):
    doc[_DP] = _data_product_docs_to_model(doc[_DP])
    doc[_MTC] = _matcher_docs_to_model(doc.get(_MTC))
    return models.ActiveCollection.construct(**remove_arango_keys(doc))


def _doc_to_saved_coll(doc: dict):
    doc[_DP] = _data_product_docs_to_model(doc[_DP])
    doc[_MTC] = _matcher_docs_to_model(doc.get(_MTC))
    return models.SavedCollection.construct(**remove_arango_keys(doc))


def _get_and_check_col_names(dps: list[DataProductSpec]):
    col_to_id = {col: _BUILTIN for col in _COLLECTIONS}
    seen_ids = {_BUILTIN}
    collections = [] + _COLLECTIONS  # don't mutate original list
    for dp in dps:
        if dp.data_product in seen_ids:
            raise ValueError(f"duplicate data product ID: {dp.data_product}")
        seen_ids.add(dp.data_product)
        for colspec in dp.db_collections:
            if colspec.name in col_to_id:
                raise ValueError(
                    f"two data products, {dp.data_product} and {col_to_id[colspec.name]}, "
                    + f"are using the same collection, {colspec.name}")
            col_to_id[colspec.name] = dp.data_product
            collections.append(colspec.name)
    return collections
    

class ArangoStorage:
    """
    An arango wrapper for collections storage.
    """

    @classmethod
    async def create(
        cls,
        db: StandardDatabase,
        data_products: list[DataProductSpec] = None,
        create_collections_on_startup: bool = False
    ):
        """
        Create the ArangoDB wrapper.

        db - the database where the data is stored. The DB must exist.
        data_products - any data products the database must support. Collections and indexes
            are checked/created based on the data product specification.
        create_collections_on_startup - on starting the wrapper, create all collections and indexes
            rather than just checking for their existence. Usually this should be false to
            allow for system administrators to set up sharding to their liking, but auto
            creation is useful for quickly standing up a test service.
        """
        dps = data_products or []
        for colname in _get_and_check_col_names(dps):
            if create_collections_on_startup:
                await _create_collection(db, colname)
            else:
                await _check_collection_exists(db, colname)
        vercol = db.collection(COLL_SRV_VERSIONS)
        await vercol.add_persistent_index([models.FIELD_COLLECTION_ID, models.FIELD_VER_NUM])
        for dp in dps:
            for col in dp.db_collections:
                dbcol = db.collection(col.name)
                for index in col.indexes:
                    await dbcol.add_persistent_index(index)

        return ArangoStorage(db)

    def __init__(self, db: StandardDatabase):
        self._db = db


    def aql(self) -> AQL:
        """ Get the database AQL instance for running arbitrary queries. """
        return self._db.aql

    async def get_next_version(self, collection_id: str) -> int:
        """ Get the next available version number for a collection. """
        bind_vars = {
            _FLD_COLLECTION_ID: collection_id,
            f"@{_FLD_COLLECTION}": COLL_SRV_COUNTERS
        }
        aql = f"""
            UPSERT {{{FLD_ARANGO_KEY}: @{_FLD_COLLECTION_ID}}}
                INSERT {{{FLD_ARANGO_KEY}: @{_FLD_COLLECTION_ID}, {_FLD_COUNTER}: 1}}
                UPDATE {{{_FLD_COUNTER}: OLD.{_FLD_COUNTER} + 1}}
                IN @@{_FLD_COLLECTION}
                OPTIONS {{exclusive: true}}
                RETURN NEW
        """
        cur = await self._db.aql.execute(aql, bind_vars=bind_vars)
        verdoc = await cur.next()
        return verdoc[_FLD_COUNTER]

    async def get_current_version(self, collection_id: str) -> int:
        """ Get the current version counter value for a collection. """
        col = self._db.collection(COLL_SRV_COUNTERS)
        countdoc = await col.get(collection_id)
        if not countdoc:
            raise errors.NoSuchCollectionError(f"There is no collection {collection_id}")
        return countdoc[_FLD_COUNTER]

    async def save_collection_version(self, collection: models.SavedCollection) -> None:
        """
        Save a version of a collection. The version is not active.
        The caller is expected to use the `get_next_version` method to get a version number for
        the collection.
        """
        doc = collection.dict()
        # ver_tag is pretty unconstrained so MD5 to get rid of any weird characters
        doc[FLD_ARANGO_KEY] = _version_key(collection.id, collection.ver_tag)
        col = self._db.collection(COLL_SRV_VERSIONS)
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
        doc[FLD_ARANGO_KEY] = collection.id
        col = self._db.collection(COLL_SRV_ACTIVE)
        await col.insert(doc, overwrite=True)

    async def get_collection_ids(self, all_=False):
        """
        Get the IDs for active collections in the service.

        all_ - get the IDs for inactive collections as well.
        """
        bind_vars = {
            f"@{_FLD_COLLECTION}": COLL_SRV_COUNTERS if all_ else COLL_SRV_ACTIVE
        }
        aql = f"""
            FOR d in @@{_FLD_COLLECTION}
                SORT d.{FLD_ARANGO_KEY} ASC
                RETURN d.{FLD_ARANGO_KEY}
            """
        cur = await self._db.aql.execute(aql, bind_vars=bind_vars)
        return [d async for d in cur]

    async def get_collections_active(self) -> list[models.ActiveCollection]:
        # Will probably want alternate sorts in the future, will need indexes etc.
        # Cross bridge etc.
        aql = f"""
            FOR d in @@{_FLD_COLLECTION}
                SORT d.{FLD_ARANGO_KEY} ASC
                RETURN d
            """
        cur = await self._db.aql.execute(aql, bind_vars={f"@{_FLD_COLLECTION}": COLL_SRV_ACTIVE})
        return [_doc_to_active_coll(d) async for d in cur]

    async def get_collection_active(self, collection_id: str) -> models.ActiveCollection:
        """ Get an active collection. """
        col = self._db.collection(COLL_SRV_ACTIVE)
        doc = await col.get(collection_id)
        if doc is None:
            raise errors.NoSuchCollectionError(
                f"There is no active collection {collection_id}")
        return _doc_to_active_coll(doc)

    async def has_collection_version_by_tag(self, collection_id: str, ver_tag: str) -> bool:
        """ Check if a collection version exists. """
        col = self._db.collection(COLL_SRV_VERSIONS)
        return await col.has(_version_key(collection_id, ver_tag))

    async def get_collection_version_by_tag(self, collection_id: str, ver_tag: str
    ) -> models.SavedCollection:
        """ Get a collection version by its version tag. """
        col = self._db.collection(COLL_SRV_VERSIONS)
        doc = await col.get(_version_key(collection_id, ver_tag))
        if doc is None:
            raise errors.NoSuchCollectionVersionError(
                f"No collection {collection_id} with version tag {ver_tag}")
        # if we really want to be careful here we could check that the id and tag match
        # the returned doc, since theoretically it might be possible to construct an id and tag
        # that collide with the md5 of another id and tag. YAGNI for now.
        return _doc_to_saved_coll(doc)

    async def get_collection_version_by_num(self, collection_id: str, ver_num: int
    ) -> models.SavedCollection:
        """ Get a collection version by its version number. """
        col = self._db.collection(COLL_SRV_VERSIONS)
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
        return _doc_to_saved_coll(doc)
    
    async def get_collection_versions(
        self, collection_id: str, max_ver: int = None, limit: int = 1000
    ) -> list[models.SavedCollection]:
        """
        List versions of a collection, sorted from highest to lowest.

        collection_id - the ID of the collection.
        max_ver - the maximum version to return. This can be used to page through the results.
        limit - the maximum number of versions to return. Default and maximum value is 1000.
        """
        if not limit or limit < 1:
            limit = 1000
        if limit > 1000:
            raise errors.IllegalParameterError("Limit must be <= 1000")
        if not max_ver or max_ver < 1:
            max_ver = None
        bind_vars = {
            f"@{_FLD_COLLECTION}": COLL_SRV_VERSIONS,
            _FLD_COLLECTION_ID: collection_id,
            _FLD_LIMIT: limit
        }
        aql = f"""
                FOR d IN @@{_FLD_COLLECTION}
                    FILTER d.{models.FIELD_COLLECTION_ID} == @{_FLD_COLLECTION_ID}
              """
        if max_ver:
            bind_vars[_FLD_VER_NUM] = max_ver
            aql += f"""
                    FILTER d.{models.FIELD_VER_NUM} <= @{_FLD_VER_NUM}
                    """
        aql += f"""
                    SORT d.{models.FIELD_VER_NUM} DESC
                    LIMIT @{_FLD_LIMIT}
                    RETURN d
                """
        cur = await self._db.aql.execute(aql, bind_vars=bind_vars)
        return [_doc_to_saved_coll(d) async for d in cur]

    async def save_match(self, match: models.InternalMatch) -> models.Match:
        """
        Save a collection match. If the match already exists (based on the match ID),
        that match is updated with the user permissions and last access data from the incoming
        match (using the last access date for both the permissions check and access date) and
        the updated match is returned.
        """
        if len(match.user_last_perm_check) != 1:
            raise ValueError(f"There must be exactly one user in {models.FIELD_MATCH_USER_PERMS}")
        doc = jsonable_encoder(match)
        doc[FLD_ARANGO_KEY] = match.match_id
        # So it turns out that upsert is non-atomic, sigh
        # https://www.arangodb.com/docs/stable/aql/examples-upsert-repsert.html#upsert-is-non-atomic
        # As such, all the code needed to make an upsert work properly with bind variables
        # and large documents here seems like a waste of time.
        # Seems easier to just try to insert, and if that fails, update & get the current doc
        col = self._db.collection(COLL_SRV_MATCHES)
        try:
            await col.insert(doc)
            return models.Match.construct(**doc)
        except DocumentInsertError as e:
            if e.error_code == _ARANGO_ERR_UNIQUE_CONSTRAINT:
                username = next(iter(match.user_last_perm_check.keys()))
                try:
                    return await self.update_match_permissions_check(
                        match.match_id, username, match.last_access)
                except errors.NoSuchMatchError as e:
                    # This is highly unlikely. Not worth spending any time trying to recover
                    raise ValueError(
                        "Well, I tried. Either something is very wrong with the "
                        + "database or I just got really unlucky with timing on a match "
                        + "deletion. Try matching again."
                    ) from e
            else:
                raise e

    async def update_match_permissions_check(self, match_id: str, username: str, check_time: int):
        """
        Update the last time permissions were checked for a user for a match.
        Also updates the overall document access time.
        Throws an error if the match doesn't exist.

        match_id - the ID of the match.
        username - the user name of the user whose permissions were checked.
        check_time - the time at which the permisisons were checked.
        """
        aql = f"""
            FOR d in @@{_FLD_COLLECTION}
                FILTER d.{FLD_ARANGO_KEY} == @{_FLD_MATCH_ID}
                UPDATE d WITH {{
                    {models.FIELD_MATCH_LAST_ACCESS}: @CHECK_TIME,
                    {models.FIELD_MATCH_USER_PERMS}: {{@USERNAME: @CHECK_TIME}}
                }} IN @@{_FLD_COLLECTION}
                OPTIONS {{exclusive: true}}
                LET updated = NEW
                RETURN KEEP(updated, @KEEP_LIST)
            """
        bind_vars = {
            f"@{_FLD_COLLECTION}": COLL_SRV_MATCHES,
            _FLD_MATCH_ID: match_id,
            "USERNAME": username,
            "CHECK_TIME": check_time,
            "KEEP_LIST": list(models.Match.__fields__.keys()),
        }
        # TODO TEST test no match - is it empty cur or something else?
        cur = await self._db.aql.execute(aql, bind_vars=bind_vars)
        # having a count > 1 is impossible since keys are unique
        if cur.empty():
            raise errors.NoSuchMatchError(match_id)
        doc = await cur.next()
        return models.Match.construct(**doc)
