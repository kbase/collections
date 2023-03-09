"""
A storage system for collections based on an Arango backend.
"""

### Notes ###
# 1:
# So it turns out that upsert is non-atomic, sigh
# https://www.arangodb.com/docs/stable/aql/examples-upsert-repsert.html#upsert-is-non-atomic
# As such, all the code needed to make an upsert work properly with bind variables
# and large documents here seems like a waste of time.
# Seems easier to just try to insert, and if that fails, update & get the current doc
#
#

from aioarango.aql import AQL
from aioarango.database import StandardDatabase
from aioarango.exceptions import CollectionCreateError, DocumentInsertError
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from typing import Any, Callable, Awaitable
from src.common.hash import md5_string
from src.common.storage.collection_and_field_names import (
    FLD_ARANGO_KEY,
    FLD_ARANGO_ID,
    COLL_SRV_ACTIVE,
    COLL_SRV_COUNTERS,
    COLL_SRV_VERSIONS,
    COLL_SRV_MATCHES,
    COLL_SRV_MATCHES_DELETED,
    COLL_SRV_MATCHES_DATA_PRODUCTS,
)
from src.service import models
from src.service import errors
from src.service.data_products.common_models import DBCollection


# service collection names that aren't shared with data loaders.

_FLD_COLLECTION = "collection"
_FLD_COLLECTION_ID = "collection_id"
_FLD_MATCH_ID = "match_id"
_FLD_CHECK_TIME = "check_time"
_FLD_COUNTER = "counter"
_FLD_VER_NUM = "ver_num"
_FLD_LIMIT = "limit"

_ARANGO_SPECIAL_KEYS = [FLD_ARANGO_KEY, FLD_ARANGO_ID, "_rev"]
ARANGO_ERR_NAME_EXISTS = 1207
_ARANGO_ERR_UNIQUE_CONSTRAINT = 1210

_COLLECTIONS = [
    COLL_SRV_ACTIVE,
    COLL_SRV_COUNTERS,
    COLL_SRV_VERSIONS,
    COLL_SRV_MATCHES,
    COLL_SRV_MATCHES_DELETED,
    COLL_SRV_MATCHES_DATA_PRODUCTS
]
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
    return models.ActiveCollection.construct(
        **models.remove_non_model_fields(doc, models.ActiveCollection)
    )


def _doc_to_saved_coll(doc: dict):
    doc[_DP] = _data_product_docs_to_model(doc[_DP])
    doc[_MTC] = _matcher_docs_to_model(doc.get(_MTC))
    return models.SavedCollection.construct(
        **models.remove_non_model_fields(doc, models.SavedCollection))


def _get_and_check_col_names(dps: dict[str, list[DBCollection]]):
    col_to_id = {col: _BUILTIN for col in _COLLECTIONS}
    collections = [] + _COLLECTIONS  # don't mutate original list
    for dpid, db_collections in dps.items():
        if dpid == _BUILTIN:
            raise ValueError(f"Cannot use name {_BUILTIN} for a data product ID")
        for colspec in db_collections:
            if colspec.name in col_to_id:
                raise ValueError(
                    f"two data products, {dpid} and {col_to_id[colspec.name]}, "
                    + f"are using the same collection, {colspec.name}")
            col_to_id[colspec.name] = dpid
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
        data_products: dict[str, list[DBCollection]] = None,
        create_collections_on_startup: bool = False
    ):
        """
        Create the ArangoDB wrapper.

        db - the database where the data is stored. The DB must exist.
        data_products - any data products the database must support as a mapping of data product
            ID to the index specifications. Collections and indexes are checked/created based
            on said specifications.
        create_collections_on_startup - on starting the wrapper, create all collections and indexes
            rather than just checking for their existence. Usually this should be false to
            allow for system administrators to set up sharding to their liking, but auto
            creation is useful for quickly standing up a test service.
        """
        dps = data_products or {}
        for colname in _get_and_check_col_names(dps):
            if create_collections_on_startup:
                await _create_collection(db, colname)
            else:
                await _check_collection_exists(db, colname)
        vercol = db.collection(COLL_SRV_VERSIONS)
        await vercol.add_persistent_index([models.FIELD_COLLECTION_ID, models.FIELD_VER_NUM])
        matchcol = db.collection(COLL_SRV_MATCHES)
        # find matches ready to be moved to the deleted state
        await matchcol.add_persistent_index([models.FIELD_MATCH_LAST_ACCESS])
        for col_list in dps.values():
            for col in col_list:
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
        try:
            verdoc = await cur.next()
        finally:
            await cur.close(ignore_missing=True)
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
        try:
            return [d async for d in cur]
        finally:
            await cur.close(ignore_missing=True)

    async def get_collections_active(self) -> list[models.ActiveCollection]:
        # Will probably want alternate sorts in the future, will need indexes etc.
        # Cross bridge etc.
        aql = f"""
            FOR d in @@{_FLD_COLLECTION}
                SORT d.{FLD_ARANGO_KEY} ASC
                RETURN d
            """
        cur = await self._db.aql.execute(aql, bind_vars={f"@{_FLD_COLLECTION}": COLL_SRV_ACTIVE})
        try:
            return [_doc_to_active_coll(d) async for d in cur]
        finally:
            await cur.close(ignore_missing=True)

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
        try:
            if cur.count() > 1:
                raise ValueError(
                    f"Found more than 1 document in the db for collection {collection_id} "
                    + f"and version number {ver_num}")
            if cur.count() < 1:
                raise errors.NoSuchCollectionVersionError(
                    f"No collection {collection_id} with version number {ver_num}")
            doc = await cur.next()
        finally:
            await cur.close(ignore_missing=True)
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
        try:
            return [_doc_to_saved_coll(d) async for d in cur]
        finally:
            await cur.close(ignore_missing=True)

    async def save_match(self, match: models.InternalMatch) -> tuple[models.Match, bool]:
        """
        Save a collection match. If the match already exists (based on the match ID),
        that match is updated with the user permissions and last access data from the incoming
        match (using the last access date for both the permissions check and access date) and
        the updated match is returned.

        Returns a tuple of the match and boolean indicating whether the match already
        existed (true) or was created anew (false)
        """
        if len(match.user_last_perm_check) != 1:
            raise ValueError(f"There must be exactly one user in {models.FIELD_MATCH_USER_PERMS}")
        doc = jsonable_encoder(match)
        doc[FLD_ARANGO_KEY] = match.match_id
        # See Note 1 at the beginning of the file
        col = self._db.collection(COLL_SRV_MATCHES)
        try:
            await col.insert(doc)
            return models.Match.construct(
                **models.remove_non_model_fields(doc, models.Match)
            ), False
        except DocumentInsertError as e:
            if e.error_code == _ARANGO_ERR_UNIQUE_CONSTRAINT:
                username = next(iter(match.user_last_perm_check.keys()))
                try:
                    return await self.update_match_permissions_check(
                        match.match_id, username, match.last_access), True
                except errors.NoSuchMatchError as e:
                    # This is highly unlikely. Not worth spending any time trying to recover
                    raise ValueError(
                        "Well, I tried. Either something is very wrong with the "
                        + "database or I just got really unlucky with timing on a match "
                        + "deletion. Try matching again."
                    ) from e
            else:
                raise e

    async def remove_match(self, match_id: str, last_access: int) -> bool:
        """
        Removes the match record from the database if the last access time is as provided.
        If the last access time does not match, it does nothing. This allows the match to be
        removed safely after some reasonable period after a last access without a race condition,
        as a new access will change the access time and prevent the match from being removed.

        Does not move the match to a deleted state or otherwise modify the database.
        Deleting matches that have not completed running is not prevented, but is generally unwise.

        match_id - the ID of the match to remove.
        last_access - the time the match was accessed last.

        Returns true if the match document was removed, false otherwise.
        """
        return await self._remove_match(match_id, last_access, COLL_SRV_MATCHES)

    async def _remove_match(self, match_id: str, last_access: int, coll: str) -> bool:
        # match_id is the match ID for regular matches, internal_match_id for deleted matches
        aql = f"""
            FOR d in @@{_FLD_COLLECTION}
                FILTER d.{FLD_ARANGO_KEY} == @{_FLD_MATCH_ID}
                FILTER d.{models.FIELD_MATCH_LAST_ACCESS} == @last_access
                REMOVE d IN @@{_FLD_COLLECTION}
                OPTIONS {{exclusive: true}}
                RETURN KEEP(d, "{FLD_ARANGO_KEY}")
            """
        bind_vars = {
            f"@{_FLD_COLLECTION}": coll,
            _FLD_MATCH_ID: match_id,
            "last_access": last_access,
        }
        cur = await self._db.aql.execute(aql, bind_vars=bind_vars)
        # having a count > 1 is impossible since keys are unique
        try:
            return not cur.empty()
        finally:
            await cur.close(ignore_missing=True)

    async def update_match_permissions_check(self, match_id: str, username: str, check_time: int
    ) -> models.Match:
        """
        Update the last time permissions were checked for a user for a match.
        Also updates the overall document access time.
        Throws an error if the match doesn't exist.

        match_id - the ID of the match.
        username - the user name of the user whose permissions were checked.
        check_time - the time at which the permisisons were checked in epoch milliseconds.
        """
        aql = f"""
            FOR d in @@{_FLD_COLLECTION}
                FILTER d.{FLD_ARANGO_KEY} == @{_FLD_MATCH_ID}
                UPDATE d WITH {{
                    {models.FIELD_MATCH_LAST_ACCESS}: @{_FLD_CHECK_TIME},
                    {models.FIELD_MATCH_USER_PERMS}: {{@USERNAME: @{_FLD_CHECK_TIME}}}
                }} IN @@{_FLD_COLLECTION}
                OPTIONS {{exclusive: true}}
                LET updated = NEW
                RETURN KEEP(updated, @KEEP_LIST)
            """
        bind_vars = {
            f"@{_FLD_COLLECTION}": COLL_SRV_MATCHES,
            _FLD_MATCH_ID: match_id,
            "USERNAME": username,
            _FLD_CHECK_TIME: check_time,
            "KEEP_LIST": list(models.Match.__fields__.keys()),
        }
        cur = await self._db.aql.execute(aql, bind_vars=bind_vars)
        # having a count > 1 is impossible since keys are unique
        try:
            if cur.empty():
                raise errors.NoSuchMatchError(match_id)
            doc = await cur.next()
        finally:
            await cur.close(ignore_missing=True)
        return models.Match.construct(**models.remove_non_model_fields(doc, models.Match))

    async def update_match_last_access(self, match_id: str, last_access: int) -> None:
        """
        Update the last time the match was accessed.
        Throws an error if the match doesn't exist.

        match_id - the ID of the match.
        check_time - the time at which the match was accessed in epoch milliseconds.
        """
        aql = f"""
            FOR d in @@{_FLD_COLLECTION}
                FILTER d.{FLD_ARANGO_KEY} == @{_FLD_MATCH_ID}
                UPDATE d WITH {{
                    {models.FIELD_MATCH_LAST_ACCESS}: @{_FLD_CHECK_TIME}
                }} IN @@{_FLD_COLLECTION}
                OPTIONS {{exclusive: true}}
                LET updated = NEW
                RETURN KEEP(updated, "{FLD_ARANGO_KEY}")
            """
        bind_vars = {
            f"@{_FLD_COLLECTION}": COLL_SRV_MATCHES,
            _FLD_MATCH_ID: match_id,
            _FLD_CHECK_TIME: last_access,
        }
        await self._execute_aql_and_check_match_exists(match_id, aql, bind_vars)

    async def send_match_heartbeat(self, match_id: str, heartbeat_timestamp: int):
        """
        Send a heartbeat to a match, updating the heartbeat timestamp.

        match_id - the ID of the match to modify.
        heartbeat_timestamp - the timestamp of the heartbeat in epoch milliseconts.
        """
        aql = f"""
            FOR d in @@{_FLD_COLLECTION}
                FILTER d.{FLD_ARANGO_KEY} == @{_FLD_MATCH_ID}
                UPDATE d WITH {{
                    {models.FIELD_MATCH_HEARTBEAT}: @heartbeat
                }} IN @@{_FLD_COLLECTION}
                OPTIONS {{exclusive: true}}
                LET updated = NEW
                RETURN KEEP(updated, "{FLD_ARANGO_KEY}")
            """
        bind_vars = {
            f"@{_FLD_COLLECTION}": COLL_SRV_MATCHES,
            _FLD_MATCH_ID: match_id,
            "heartbeat": heartbeat_timestamp,
        }
        await self._execute_aql_and_check_match_exists(match_id, aql, bind_vars)

    async def _execute_aql_and_check_match_exists(
        self,
        match_id: str,
        aql: str,
        bind_vars: dict[str, Any],
    ) -> None:
        cur = await self._db.aql.execute(aql, bind_vars=bind_vars)
        # having a count > 1 is impossible since keys are unique
        try:
            if cur.empty():
                raise errors.NoSuchMatchError(match_id)
        finally:
            await cur.close(ignore_missing=True)

    def _correct_match_doc_in_place(self, doc: dict[str, Any]):
        doc[models.FIELD_MATCH_STATE] = models.ProcessState(doc[models.FIELD_MATCH_STATE])

    async def _get_match(self, coll: str, match_id: str, exception: bool = True):
        col = self._db.collection(coll)
        doc = await col.get(match_id)
        if doc is None:
            if not exception:
                return None
            raise errors.NoSuchMatchError(match_id)
        self._correct_match_doc_in_place(doc)
        return doc

    async def get_match(self, match_id: str, verbose: bool = False) -> models.MatchVerbose:
        """
        Get a match.

        match_id - the ID of the match to get.
        verbose - include the UPAs and matching IDs, default false. If false, the UPA and matching
            ID fields are empty.
        """
        # could potentially speed things up a bit and reduce bandwidth by using AQL and
        # KEEP(). Don't bother for now.
        doc = await self._get_match(COLL_SRV_MATCHES, match_id)
        match = models.MatchVerbose.construct(
            **models.remove_non_model_fields(doc, models.MatchVerbose))
        if not verbose:
            match.upas = []
            match.matches = []
        return match

    def _to_internal_match(self, doc: dict[str, Any]) -> models.InternalMatch:
        self._correct_match_doc_in_place(doc)
        return models.InternalMatch.construct(
            **models.remove_non_model_fields(doc, models.InternalMatch))

    async def get_match_full(self, match_id: str, exception: bool = True) -> models.InternalMatch:
        """
        Get the full match associated with the match id.

        match_id - the ID of the match.
        exception - True to throw an exception if the match is missing, False to return None.
        """
        doc = await self._get_match(COLL_SRV_MATCHES, match_id, exception)
        return None if not doc else self._to_internal_match(doc)

    async def update_match_state(
        self,
        match_id: str,
        match_state: models.ProcessState,
        update_time: int,
        matches: list[str] = None
    ) -> None:
        """
        Update the state of the match, optionally setting match IDs.

        match_id - the ID of the match to update
        match_state - the state of the match to set
        update_time - the time at which the match state was updated in epoch milliseconds
        matches - the matches to add to the match
        """
        bind_vars = {
            f"@{_FLD_COLLECTION}": COLL_SRV_MATCHES,
            _FLD_MATCH_ID: match_id,
            "match_state": match_state.value,
            "update_time": update_time,
        }
        aql = f"""
            FOR d in @@{_FLD_COLLECTION}
                FILTER d.{FLD_ARANGO_KEY} == @{_FLD_MATCH_ID}
                UPDATE d WITH {{
                    {models.FIELD_MATCH_STATE}: @match_state,
                    {models.FIELD_MATCH_STATE_UPDATED}: @update_time,
            """
        if matches:
            aql += f"""
                    {models.FIELD_MATCH_MATCHES}: @matches,
            """
            bind_vars["matches"] = matches
        aql += f"""
                }} IN @@{_FLD_COLLECTION}
                OPTIONS {{exclusive: true}}
                LET updated = NEW
                RETURN KEEP(updated, "{FLD_ARANGO_KEY}")
            """
        await self._execute_aql_and_check_match_exists(match_id, aql, bind_vars)

    async def process_old_matches(
        self,
        match_max_last_access_ms: int,
        processor: Callable[[models.InternalMatch], Awaitable[None]],
    ):
        """
        Process matches with a last access date older than the given date.

        match_max_last_access_ms - process matches with a last acess date older that this in epoch
            milliseconds.
        processor - an async callable to which each match will be provided in turn.
        """
        aql = f"""
            FOR d IN @@{_FLD_COLLECTION}
                FILTER d.{models.FIELD_MATCH_LAST_ACCESS} < @max_last_access
                RETURN d
            """
        bind_vars = {
            f"@{_FLD_COLLECTION}": COLL_SRV_MATCHES,
            "max_last_access": match_max_last_access_ms,
        }
        cur = await self._db.aql.execute(aql, bind_vars=bind_vars)
        try:
            async for d in cur:
                await processor(self._to_internal_match(d))
        finally:
            await cur.close(ignore_missing=True)

    async def add_deleted_match(self, match: models.DeletedMatch):
        """
        Adds a match in the deleted state to the database. This will overwrite any deleted match
        already present with the same internal match ID. Does not alter the source match.
        """
        doc = jsonable_encoder(match)
        doc[FLD_ARANGO_KEY] = match.internal_match_id
        col = self._db.collection(COLL_SRV_MATCHES_DELETED)
        await col.insert(doc, overwrite=True, silent=True)

    def _to_deleted_match(self, doc: dict[str, Any]) -> models.DeletedMatch:
        self._correct_match_doc_in_place(doc)
        return models.DeletedMatch.construct(
            **models.remove_non_model_fields(doc, models.DeletedMatch))

    async def get_deleted_match(self, internal_match_id: str) -> models.DeletedMatch:
        """
        Get a match in the deleted state from the database given its internal match ID.
        """
        doc = await self._get_match(COLL_SRV_MATCHES_DELETED, internal_match_id)
        return self._to_deleted_match(doc)

    async def remove_deleted_match(self, internal_match_id: str, last_access: int) -> bool:
        """
        Removes the deleted match record from the database if the last access time is as provided.
        If the last access time does not match, it does nothing. This allows the deleted match to
        be removed safely without causing a race condition if another match deletion thread
        updates the deleted match record in between retrieving the match from the database.

        Does not otherwise modify the database.

        internal_match_id - the internal ID of the match to remove.
        last_access - the time the match was accessed last.

        Returns true if the match document was removed, false otherwise.
        """
        return await self._remove_match(internal_match_id, last_access, COLL_SRV_MATCHES_DELETED)
    
    async def process_deleted_matches(
        self,
        processor: Callable[[models.DeletedMatch], Awaitable[None]]
    ):
        """
        Process deleted matches.

        processor - an async callable to which each match will be provided in turn.
        """
        col = self._db.collection(COLL_SRV_MATCHES_DELETED)
        cur = await col.all()
        try:
            async for d in cur:
                await processor(self._to_deleted_match(d))
        finally:
            await cur.close(ignore_missing=True)

    def _data_product_match_key(self, internal_match_id: str, data_product: str) -> str:
        return f"{data_product}_{internal_match_id}"

    async def create_or_get_data_product_match(self, dp_match: models.DataProductMatchProcess
    ) -> tuple[models.DataProductMatchProcess, bool]:
        """
        Save the data product match process to the database if it doesn't already exist, or
        get the current match if it does.

        Returns a tuple of the match as it currently exists in the database and a boolean
        indicating whether it was created or already existed.
        """
        key = self._data_product_match_key(dp_match.internal_match_id, dp_match.data_product)
        doc = dp_match.dict()
        doc[FLD_ARANGO_KEY] = key
        # See Note 1 at the beginning of the file
        col = self._db.collection(COLL_SRV_MATCHES_DATA_PRODUCTS)
        try:
            await col.insert(doc)
            return dp_match, False
        except DocumentInsertError as e:
            if e.error_code == _ARANGO_ERR_UNIQUE_CONSTRAINT:
                doc = await col.get({FLD_ARANGO_KEY: key})
                if not doc:
                    # This is highly unlikely. Not worth spending any time trying to recover
                    raise ValueError(
                        "Well, I tried. Either something is very wrong with the "
                        + "database or I just got really unlucky with timing on a match "
                        + "deletion. Try matching again.")
                doc[models.FIELD_DATA_PRODUCT_MATCH_STATE] = models.ProcessState(
                    doc[models.FIELD_DATA_PRODUCT_MATCH_STATE])
                return models.DataProductMatchProcess.construct(
                    **models.remove_non_model_fields(doc, models.DataProductMatchProcess)), True
            raise e

    async def send_data_product_match_heartbeat(
        self,
        internal_match_id: str,
        data_product: str,
        heartbeat_timestamp: int):
        """
        Send a heartbeat to a data prodduct match, updating the heartbeat timestamp.

        internal_match_id - the ID of the data product match to update
        data_product - the data product performing the match
        heartbeat_timestamp - the timestamp of the heartbeat in epoch milliseconts.
        """
        key = self._data_product_match_key(internal_match_id, data_product)
        bind_vars = {
            f"@{_FLD_COLLECTION}": COLL_SRV_MATCHES_DATA_PRODUCTS,
            "key": key,
            "heartbeat": heartbeat_timestamp,
        }
        aql = f"""
            FOR d in @@{_FLD_COLLECTION}
                FILTER d.{FLD_ARANGO_KEY} == @key
                UPDATE d WITH {{
                    {models.FIELD_MATCH_HEARTBEAT}: @heartbeat
                }} IN @@{_FLD_COLLECTION}
                OPTIONS {{exclusive: true}}
                LET updated = NEW
                RETURN KEEP(updated, "{FLD_ARANGO_KEY}")
            """
        await self._execute_aql_and_check_match_exists(key, aql, bind_vars)

    async def update_data_product_match_state(
        self,
        internal_match_id: str,
        data_product: str,
        match_state: models.ProcessState,
        update_time: int,
    ) -> None:
        """
        Update the state of a data product match process.

        internal_match_id - the ID of the data product match to update
        data_product - the data product performing the match
        match_state - the state of the match to set
        update_time - the time at which the match state was updated in epoch milliseconds
        """
        key = self._data_product_match_key(internal_match_id, data_product)
        bind_vars = {
            f"@{_FLD_COLLECTION}": COLL_SRV_MATCHES_DATA_PRODUCTS,
            "key": key,
            "match_state": match_state.value,
            "update_time": update_time,
        }
        aql = f"""
            FOR d in @@{_FLD_COLLECTION}
                FILTER d.{FLD_ARANGO_KEY} == @key
                UPDATE d WITH {{
                    {models.FIELD_DATA_PRODUCT_MATCH_STATE}: @match_state,
                    {models.FIELD_DATA_PRODUCT_MATCH_STATE_UPDATED}: @update_time,
                }} IN @@{_FLD_COLLECTION}
                OPTIONS {{exclusive: true}}
                LET updated = NEW
                RETURN KEEP(updated, "{FLD_ARANGO_KEY}")
            """
        await self._execute_aql_and_check_match_exists(key, aql, bind_vars)

    async def remove_data_product_match(self, internal_match_id: str, data_product: str):
        """
        Remove a data product match document.

        internal_match_id - the internal match ID for the match.
        data_product - the data product ID.
        """
        key = self._data_product_match_key(internal_match_id, data_product)
        col = self._db.collection(COLL_SRV_MATCHES_DATA_PRODUCTS)
        await col.delete(key, ignore_missing=True, silent=True)

    async def import_bulk_ignore_collisions(self, arango_collection: str, documents: dict[str, Any]
    ) -> None:
        """
        Import many documents to an arango collection. Any collisions are ignored, so callers
        of this method should ensure that a document with a given key will always contain the
        same data.
        """
        col = self._db.collection(arango_collection)
        await col.import_bulk(documents, on_duplicate="ignore")
