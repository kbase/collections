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

from aioarango.cursor import Cursor
from aioarango.database import StandardDatabase
from aioarango.exceptions import (
    CollectionCreateError,
    DocumentInsertError,
    ViewCreateError,
    ViewGetError,
)
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from typing import Any, Callable, Awaitable, Self
from src.common.hash import md5_string
from src.common.product_models.columnar_attribs_common_models import (
    ColumnarAttributesSpec,
    FilterStrategy
)
from src.common.storage import collection_and_field_names as names
from src.service import models
from src.service import errors
from src.service.data_products.common_models import DataProductSpec


_ERRMAP = {
    models.SubsetType.MATCH: errors.NoSuchMatchError,
    models.SubsetType.SELECTION: errors.NoSuchSelectionError,
}


# service collection names that aren't shared with data loaders.

_FLD_COLLECTION = "collection"
_FLD_COLLECTION_ID = "collection_id"
_FLD_MATCH_ID = "match_id"
_FLD_CHECK_TIME = "check_time"
_FLD_COUNTER = "counter"
_FLD_VER_NUM = "ver_num"
_FLD_LIMIT = "limit"

_ARANGO_SPECIAL_KEYS = [names.FLD_ARANGO_KEY, names.FLD_ARANGO_ID, "_rev"]
_ARANGO_ERR_COLL_OR_VIEW_NOT_FOUND = 1203
ARANGO_ERR_NAME_EXISTS = 1207
_ARANGO_ERR_UNIQUE_CONSTRAINT = 1210

_COLLECTIONS = [  # Might want to define this in names?
    names.COLL_SRV_CONFIG,
    names.COLL_SRV_ACTIVE,
    names.COLL_SRV_COUNTERS,
    names.COLL_SRV_VERSIONS,
    names.COLL_SRV_MATCHES,
    names.COLL_SRV_MATCHES_DELETED,
    names.COLL_SRV_DATA_PRODUCT_PROCESSES,
    names.COLL_SRV_SELECTIONS,
    names.COLL_SRV_SELECTIONS_DELETED,
    names.COLL_EXPORT_TYPES,
]
_BUILTIN = "builtin"
_DYNCFG_KEY = "dynconfig"

class ViewExistsError(Exception):
    """
    Thrown when an ArangoSearch view already exists and does not match the provided
    view specification.
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


def _get_and_check_col_names(dps: list[DataProductSpec]):
    col_to_id = {col: _BUILTIN for col in _COLLECTIONS}
    collections = [] + _COLLECTIONS  # don't mutate original list
    for dp in dps:
        if dp.data_product == _BUILTIN:
            raise ValueError(f"Cannot use name {_BUILTIN} for a data product ID")
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
    ) -> Self:
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
        dps = data_products or []
        for colname in _get_and_check_col_names(dps):
            if create_collections_on_startup:
                await _create_collection(db, colname)
            else:
                await _check_collection_exists(db, colname)
        
        vercol = db.collection(names.COLL_SRV_VERSIONS)
        await vercol.add_persistent_index([models.FIELD_COLLECTION_ID, models.FIELD_VER_NUM])
        
        matchcol = db.collection(names.COLL_SRV_MATCHES)
        # find matches ready to be moved to the deleted state
        await matchcol.add_persistent_index([models.FIELD_LAST_ACCESS])
        # find matches by internal match ID
        await matchcol.add_persistent_index([models.FIELD_MATCH_INTERNAL_MATCH_ID])
        # find matches by collection version
        await matchcol.add_persistent_index(
            [models.FIELD_COLLSPEC_COLLECTION_ID, models.FIELD_COLLSPEC_COLLECTION_VER])

        selcol = db.collection(names.COLL_SRV_SELECTIONS)
        # find selections ready to be moved to the deleted state
        await selcol.add_persistent_index([models.FIELD_LAST_ACCESS])
        # find selections by internal selection ID
        await selcol.add_persistent_index([models.FIELD_SELECTION_INTERNAL_SELECTION_ID])
        # find selections by collection version
        await selcol.add_persistent_index(
            [models.FIELD_COLLSPEC_COLLECTION_ID, models.FIELD_COLLSPEC_COLLECTION_VER])
        
        typescol = db.collection(names.COLL_EXPORT_TYPES)
        await typescol.add_persistent_index(
            [names.FLD_COLLECTION_ID, names.FLD_DATA_PRODUCT, names.FLD_LOAD_VERSION]
        )
        
        for dp in dps:
            for col in dp.db_collections:
                dbcol = db.collection(col.name)
                for index in col.indexes:
                    await dbcol.add_persistent_index(index)

        return ArangoStorage(db)

    def __init__(self, db: StandardDatabase):
        self._db = db

    async def execute_aql(self, aql_str: str, bind_vars: dict[str, Any] = None, count: bool = False
    ) -> Cursor:
        """
        Execute an aql statement.

        aql_str - the AQL string to execute.
        bind_vars - any bind variables for the AQL string.
        count - True to return the total count for the match. This can be significantly more
             expensive than the query so use the option wisely.
        """
        return await self._db.aql.execute(aql_str, bind_vars=bind_vars or {}, count=count)
    
    async def create_analyzer(
        self, name: str, type_: str, properties: dict[str, Any] = None, features: list[str] = None
    ):
        """
        Create an ArangoSearch analyzer.
        See https://docs.arangodb.com/3.11/index-and-search/analyzers/
        
        name - the name of the analyzer.
        type_ - the type of the analyzer.
        properties - the properties of the analyzer
        features - the features for the analyzer.
        """
        await self._db.create_analyzer(name, type_, properties, features)
    
    async def create_search_view(
        self,
        name: str,
        arango_collection: str,
        view_spec: ColumnarAttributesSpec,
        analyzer_provider: Callable[[FilterStrategy, bool], str],
        include_all_fields: bool = False
    ):
        """
        Create a search view for a collection.
        
        name - the name of the view to create.
        arango_collection - the collection name for which to create the view.
        view_spec - the specification for the view to create.
        analyzer_provider - a function, that given a filter strategy, provides the name of
            an analyzer to use for that strategy. The second argument defines whether to
            return None (True) or the name of the default analyzer (False) when the default
            analyzer is to be returned.
        include_all_fields - whether to set include_all_fields to true for the created search view.
        """
        view_fields = self._view_spec_to_fields(view_spec, analyzer_provider)
        try:
            await self._db.create_arangosearch_view(
                name, {"links": {arango_collection: {
                                                    "fields": view_fields,
                                                    "includeAllFields": include_all_fields}}}
            )
        except ViewCreateError as e:
            if e.error_code == ARANGO_ERR_NAME_EXISTS:
                view = await self._db.view(name)
                if view["links"][arango_collection]["fields"] != view_fields:
                    raise ViewExistsError(f"The view '{name}' already exists and differs from "
                        + "the requested specification.") from e
            else:
                raise
    
    def _view_spec_to_fields(
            self,
            view_spec: ColumnarAttributesSpec,
            analyzer_provider: Callable[[FilterStrategy, bool], str]
        ) -> dict[str, Any]:
        fields = {}
        for colattrib in view_spec.columns:
            analyzer = analyzer_provider(colattrib.filter_strategy, True)
            if not analyzer:
                fields[colattrib.key] = {}
            else:
                # we may want > 1 analyzer per field at some point, deal with that when it happens.
                fields[colattrib.key] = {"analyzers": [analyzer]}
        return fields
    
    async def get_search_views_from_spec(
        self,
        arango_collection: str,
        view_spec: ColumnarAttributesSpec,
        analyzer_provider: Callable[[FilterStrategy, bool], str],
        include_all_fields: bool = False
        ) -> list:
        """
        Given a view spec, find a matching views in a collection if any.
        
        arango_collection - the collection name associated with the view.
        view_spec - the specification for the view to find.
        analyzer_provider - a function, that given a filter strategy, provides the name of
            an analyzer to use for that strategy. The second argument defines whether to
            return None (True) or the name of the default analyzer (False) when the default
            analyzer is to be returned.
        include_all_fields - returned matching views must have the include_all_fields flag set.
            
        Returns the names of any matching views found.
        """
        view_fields = self._view_spec_to_fields(view_spec, analyzer_provider)
        views = await self._db.views()
        ret = []
        for v in views:
            view = await self._db.view(v["name"])
            if arango_collection in view["links"]:
                if (view["links"][arango_collection]["fields"] == view_fields and
                        view["links"][arango_collection]["include_all_fields"] == include_all_fields):
                    ret.append(v["name"])
        return ret
    
    async def has_search_view(self, name) -> bool:
        """ Check if a search view exists by name. """
        try:
            await self._db.view(name)
            return True
        except ViewGetError as e:
            if e.error_code == _ARANGO_ERR_COLL_OR_VIEW_NOT_FOUND:
                return False
            else:
                raise
    
    async def get_search_views(self, arango_collection: str) -> set[str]:
        """
        Get the list of views that exist for an arango collection.
        """
        ret = set()
        views = await self._db.views()
        for v in views: 
            view = await self._db.view(v["name"])
            if arango_collection in view["links"]:
                ret.add(v["name"])
        return ret
    
    async def get_dynamic_config(self) -> models.DynamicConfig:
        """ Get the dynamic configuration from the database. """
        col = self._db.collection(names.COLL_SRV_CONFIG)
        doc = await col.get(_DYNCFG_KEY)
        if not doc:
            return models.DynamicConfig()  # use default values
        return models.DynamicConfig(**doc)

    async def update_dynamic_config(self, cfg: models.DynamicConfig) -> models.DynamicConfig:
        """
        Update the dynamic configuration, overwriting any existing config keys but 
        leaving non-conflicting keys alone.
        
        Returns the updated config.
        """
        if cfg.is_empty():
            return
        doc = cfg.model_dump() | {names.FLD_ARANGO_KEY: _DYNCFG_KEY}
        bind_vars = {"@coll": names.COLL_SRV_CONFIG, "cfg": doc}
        aql = f"""
            UPSERT {{{names.FLD_ARANGO_KEY}: "{_DYNCFG_KEY}"}}
                INSERT @cfg
                UPDATE MERGE_RECURSIVE(OLD, @cfg)
                IN @@coll
                OPTIONS {{exclusive: true}}
            RETURN NEW
        """
        cur = await self.execute_aql(aql, bind_vars, count=True)
        try:
            doc = await cur.next()
            return models.DynamicConfig(**doc)
        finally:
            await cur.close(ignore_missing=True)

    async def get_next_version(self, collection_id: str) -> int:
        """ Get the next available version number for a collection. """
        bind_vars = {
            _FLD_COLLECTION_ID: collection_id,
            f"@{_FLD_COLLECTION}": names.COLL_SRV_COUNTERS
        }
        aql = f"""
            UPSERT {{{names.FLD_ARANGO_KEY}: @{_FLD_COLLECTION_ID}}}
                INSERT {{{names.FLD_ARANGO_KEY}: @{_FLD_COLLECTION_ID}, {_FLD_COUNTER}: 1}}
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
        col = self._db.collection(names.COLL_SRV_COUNTERS)
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
        doc = jsonable_encoder(collection)
        # ver_tag is pretty unconstrained so MD5 to get rid of any weird characters
        doc[names.FLD_ARANGO_KEY] = _version_key(collection.id, collection.ver_tag)
        col = self._db.collection(names.COLL_SRV_VERSIONS)
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
        The caller is expected to retrieve a collection from a `get_collection_version_by_*`
        method, update it to an active collection, and save it here.
        """
        await self._insert_model(collection, collection.id, names.COLL_SRV_ACTIVE, overwrite=True)

    async def get_collection_ids(self, all_=False):
        """
        Get the IDs for active collections in the service.

        all_ - get the IDs for inactive collections as well.
        """
        bind_vars = {
            f"@{_FLD_COLLECTION}": names.COLL_SRV_COUNTERS if all_ else names.COLL_SRV_ACTIVE
        }
        aql = f"""
            FOR d in @@{_FLD_COLLECTION}
                SORT d.{names.FLD_ARANGO_KEY} ASC
                RETURN d.{names.FLD_ARANGO_KEY}
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
                SORT d.{names.FLD_ARANGO_KEY} ASC
                RETURN d
            """
        cur = await self._db.aql.execute(
            aql, bind_vars={f"@{_FLD_COLLECTION}": names.COLL_SRV_ACTIVE})
        try:
            return [_doc_to_active_coll(d) async for d in cur]
        finally:
            await cur.close(ignore_missing=True)

    async def get_collection_active(self, collection_id: str) -> models.ActiveCollection:
        """ Get an active collection. """
        col = self._db.collection(names.COLL_SRV_ACTIVE)
        doc = await col.get(collection_id)
        if doc is None:
            raise errors.NoSuchCollectionError(
                f"There is no active collection {collection_id}")
        return _doc_to_active_coll(doc)

    async def has_collection_version_by_tag(self, collection_id: str, ver_tag: str) -> bool:
        """ Check if a collection version exists. """
        col = self._db.collection(names.COLL_SRV_VERSIONS)
        return await col.has(_version_key(collection_id, ver_tag))

    async def get_collection_version_by_tag(self, collection_id: str, ver_tag: str
    ) -> models.SavedCollection:
        """ Get a collection version by its version tag. """
        col = self._db.collection(names.COLL_SRV_VERSIONS)
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
        col = self._db.collection(names.COLL_SRV_VERSIONS)
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
            f"@{_FLD_COLLECTION}": names.COLL_SRV_VERSIONS,
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
        doc[names.FLD_ARANGO_KEY] = match.match_id
        # See Note 1 at the beginning of the file
        col = self._db.collection(names.COLL_SRV_MATCHES)
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
        return await self._remove_subset(match_id, last_access, names.COLL_SRV_MATCHES)

    async def _remove_subset(self, subset_id: str, last_access: int, coll: str) -> bool:
        aql = f"""
            FOR d in @@{_FLD_COLLECTION}
                FILTER d.{names.FLD_ARANGO_KEY} == @subset_id
                FILTER d.{models.FIELD_LAST_ACCESS} == @last_access
                REMOVE d IN @@{_FLD_COLLECTION}
                OPTIONS {{exclusive: true}}
                RETURN KEEP(d, "{names.FLD_ARANGO_KEY}")
            """
        bind_vars = {
            f"@{_FLD_COLLECTION}": coll,
            "subset_id": subset_id,
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
        check_time - the time at which the permissions were checked in epoch milliseconds.
        """
        aql = f"""
            FOR d in @@{_FLD_COLLECTION}
                FILTER d.{names.FLD_ARANGO_KEY} == @{_FLD_MATCH_ID}
                UPDATE d WITH {{
                    {models.FIELD_LAST_ACCESS}: @{_FLD_CHECK_TIME},
                    {models.FIELD_MATCH_USER_PERMS}: {{@USERNAME: @{_FLD_CHECK_TIME}}}
                }} IN @@{_FLD_COLLECTION}
                OPTIONS {{exclusive: true}}
                LET updated = NEW
                RETURN KEEP(updated, @KEEP_LIST)
            """
        bind_vars = {
            f"@{_FLD_COLLECTION}": names.COLL_SRV_MATCHES,
            _FLD_MATCH_ID: match_id,
            "USERNAME": username,
            _FLD_CHECK_TIME: check_time,
            "KEEP_LIST": list(models.Match.__fields__.keys()),  # @UndefinedVariable
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
        last_access - the time at which the match was accessed in epoch milliseconds.
        """
        await self._update_last_access(
            match_id, names.COLL_SRV_MATCHES, last_access, errors.NoSuchMatchError)

    async def _update_last_access(
        self,
        item_id: str,
        collection: str,
        last_access: int,
        errclass,
    ) -> None:
        aql = f"""
            FOR d in @@{_FLD_COLLECTION}
                FILTER d.{names.FLD_ARANGO_KEY} == @item_id
                UPDATE d WITH {{
                    {models.FIELD_LAST_ACCESS}: @{_FLD_CHECK_TIME}
                }} IN @@{_FLD_COLLECTION}
                OPTIONS {{exclusive: true}}
                RETURN NEW
            """
        bind_vars = {
            f"@{_FLD_COLLECTION}": collection,
            "item_id": item_id,
            _FLD_CHECK_TIME: last_access,
        }
        return await self._execute_aql_and_check_item_exists(aql, bind_vars, item_id, errclass)

    async def send_match_heartbeat(self, internal_match_id: str, heartbeat_timestamp: int):
        """
        Send a heartbeat to a match, updating the heartbeat timestamp.

        internal_match_id - the internal ID of the match to modify.
        heartbeat_timestamp - the timestamp of the heartbeat in epoch milliseconds.
        """
        await self._send_heartbeat(
            names.COLL_SRV_MATCHES,
            internal_match_id,
            heartbeat_timestamp,
            errors.NoSuchMatchError,
            filter_key=models.FIELD_MATCH_INTERNAL_MATCH_ID,
        )

    async def _send_heartbeat(
        self,
        collection: str,
        key: str,
        heartbeat_timestamp: int,
        errclass,
        filter_key: str = names.FLD_ARANGO_KEY
    ):
        aql = f"""
            FOR d in @@{_FLD_COLLECTION}
                FILTER d.{filter_key} == @key
                UPDATE d WITH {{
                    {models.FIELD_PROCESS_HEARTBEAT}: @heartbeat
                }} IN @@{_FLD_COLLECTION}
                OPTIONS {{exclusive: true}}
                LET updated = NEW
                RETURN KEEP(updated, "{names.FLD_ARANGO_KEY}")
            """
        bind_vars = {
            f"@{_FLD_COLLECTION}": collection,
            "key": key,
            "heartbeat": heartbeat_timestamp,
        }
        await self._execute_aql_and_check_item_exists(aql, bind_vars, key, errclass)

    async def _execute_aql_and_check_item_exists(
        self,
        aql: str,
        bind_vars: dict[str, Any],
        errstr: str,
        # TODO TYPING how do you type this? It's a class, but not an *instance* of a class,
        #             descending from errors.CollectionsError. Java would be Class<CollectionError>
        errclass,
        exception: bool = True,
    ) -> dict[str, Any] | None:
        cur = await self._db.aql.execute(aql, bind_vars=bind_vars, count=True)
        try:
            if cur.empty():
                if exception:
                    raise errclass(errstr)
                else:
                    return None
            if cur.count() > 1:
                # this should never happen, but just in case
                raise ValueError("Expected only one result")
            return await cur.next()
        finally:
            await cur.close(ignore_missing=True)


    def _correct_process_doc_in_place(self, doc: dict[str, Any]):
        doc[models.FIELD_PROCESS_STATE] = models.ProcessState(doc[models.FIELD_PROCESS_STATE])
        return doc

    async def _get_doc(self, coll: str, doc_id: str, errclass, exception: bool = True):
        col = self._db.collection(coll)
        doc = await col.get(doc_id)
        if doc is None:
            if not exception:
                return None
            raise errclass(doc_id)
        self._correct_process_doc_in_place(doc)
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
        doc = await self._get_doc(names.COLL_SRV_MATCHES, match_id, errors.NoSuchMatchError)
        match = models.MatchVerbose.construct(
            **models.remove_non_model_fields(doc, models.MatchVerbose))
        if not verbose:
            match.upas = []
            match.matches = []
        return match

    def _to_internal_match(self, doc: dict[str, Any]) -> models.InternalMatch:
        doc[models.FIELD_MATCH_WSIDS] = set(doc[models.FIELD_MATCH_WSIDS])
        return models.InternalMatch.construct(
            **models.remove_non_model_fields(doc, models.InternalMatch))

    async def get_match_full(self, match_id: str, exception: bool = True) -> models.InternalMatch:
        """
        Get the full match associated with the match id.

        match_id - the ID of the match.
        exception - True to throw an exception if the match is missing, False to return None.
        """
        doc = await self._get_doc(
            names.COLL_SRV_MATCHES, match_id, errors.NoSuchMatchError, exception)
        return None if not doc else self._to_internal_match(doc)

    async def get_match_by_internal_id(self, internal_match_id: str, exception: bool = True
    ) -> models.InternalMatch | None:
        """
        Get a match by its internal ID.

        internal_match_id - the internal ID of the match.
        exception - throw an exception if the match doesn't exist.
        """
        doc = await self._get_subset_by_internal_id(
            names.COLL_SRV_MATCHES,
            internal_match_id,
            models.FIELD_MATCH_INTERNAL_MATCH_ID,
            errors.NoSuchMatchError,
            exception=exception,
        )
        return self._to_internal_match(doc) if doc else None

    async def _get_subset_by_internal_id(
        self, coll: str, internal_id: str, field: str, errclass, exception: bool = True,
    ) -> dict[str, Any] | None:
        aql = f"""
            FOR d in @@{_FLD_COLLECTION}
                FILTER d.{field} == @internal_id
                RETURN d
            """
        bind_vars = {
            f"@{_FLD_COLLECTION}": coll,
            "internal_id": internal_id,
        }
        doc = await self._execute_aql_and_check_item_exists(
            aql, bind_vars, internal_id, errclass, exception=exception)
        return self._correct_process_doc_in_place(doc) if doc else None

    async def update_match_state(
        self,
        internal_match_id: str,
        match_state: models.ProcessState,
        update_time: int,
        matches: list[str] = None
    ) -> None:
        """
        Update the state of the match, optionally setting match IDs.

        internal_match_id - the internal ID of the match to update
        match_state - the state of the match to set
        update_time - the time at which the match state was updated in epoch milliseconds
        matches - the matches to add to the match
        """
        await self._update_state(
            internal_match_id,
            match_state,
            update_time,
            names.COLL_SRV_MATCHES,
            errors.NoSuchMatchError,
            filter_key=models.FIELD_MATCH_INTERNAL_MATCH_ID,
            data_list=matches,
            data_list_field=models.FIELD_MATCH_MATCHES,
            data_count_field=models.FIELD_MATCH_MATCH_COUNT
        )

    async def _update_state(
        self,
        data_id: str,
        state: models.ProcessState,
        update_time: int,
        collection: str,
        errclass,
        filter_key: str = names.FLD_ARANGO_KEY,
        data_list: list[str] = None,
        data_list_field: str = None,
        data_count_field: str = None,
    ):
        bind_vars = {
            f"@{_FLD_COLLECTION}": collection,
            "id": data_id,
            "state": state.value,
            "update_time": update_time,
        }
        aql = f"""
            FOR d in @@{_FLD_COLLECTION}
                FILTER d.{filter_key} == @id
                UPDATE d WITH {{
                    {models.FIELD_PROCESS_STATE}: @state,
                    {models.FIELD_PROCESS_STATE_UPDATED}: @update_time,
            """
        if data_list is not None:
            aql += f"""
                    {data_list_field}: @items,
            """
            bind_vars["items"] = data_list
            if data_count_field:
                aql += f"""
                    {data_count_field}: @count,
                """
                bind_vars["count"] = len(data_list)
        aql += f"""
                }} IN @@{_FLD_COLLECTION}
                OPTIONS {{exclusive: true}}
                LET updated = NEW
                RETURN KEEP(updated, "{names.FLD_ARANGO_KEY}")
            """
        await self._execute_aql_and_check_item_exists(aql, bind_vars, data_id, errclass)

    async def process_old_matches(
        self,
        match_max_last_access_ms: int,
        processor: Callable[[models.InternalMatch], Awaitable[None]],
    ):
        """
        Process matches with a last access date older than the given date.

        match_max_last_access_ms - process matches with a last access date older than this in epoch
            milliseconds.
        processor - an async callable to which each match will be provided in turn.
        """
        await self._process_subsets(
            names.COLL_SRV_MATCHES,
            processor,
            self._to_internal_match,
            max_last_access_ms=match_max_last_access_ms
        )

    async def process_collection_matches(
        self,
        collspec: models.CollectionSpec,
        processor: Callable[[models.InternalMatch], Awaitable[None]],
        states: set[models.ProcessState] | None = None,
    ):
        """
        Process matches in a collection.

        collspec - the collection to process and its version.
        processor - an async callable to which each match will be provided in turn.
        states - filter by match processing state (no filtering by default)
        """
        await self._process_subsets(
            names.COLL_SRV_MATCHES,
            processor,
            self._to_internal_match,
            collspec=collspec,
            states=states,    
        )

    async def _process_subsets(  # method would be parameterized in Java, meh here
        self,
        coll: str,
        processor: Callable[[models.InternalMatch | models.InternalSelection], Awaitable[None]],
        converter: Callable[[dict[str, Any]], models.InternalMatch | models.InternalSelection],
        collspec: models.CollectionSpec | None = None,
        states: set[models.ProcessState] | None = None,
        max_last_access_ms: int | None = None,
    ):
        bind_vars = {f"@{_FLD_COLLECTION}": coll}
        aql = f"""
            FOR d IN @@{_FLD_COLLECTION}
            """
        if max_last_access_ms is not None:
            aql += f"""
                FILTER d.{models.FIELD_LAST_ACCESS} < @max_last_access
                """
            bind_vars["max_last_access"] = max_last_access_ms
        if states:
            aql += f"""
                FILTER d.{models.FIELD_PROCESS_STATE} IN @states
                """
            bind_vars["states"] = [s.value for s in states]
        if collspec:
            aql += f"""
                FILTER d.{models.FIELD_COLLSPEC_COLLECTION_ID} == @collection_id
                FILTER d.{models.FIELD_COLLSPEC_COLLECTION_VER} == @collection_ver
                """
            bind_vars.update({
                "collection_id": collspec.collection_id,
                "collection_ver": collspec.collection_ver
            })
        aql += """
                RETURN d
            """
        cur = await self._db.aql.execute(aql, bind_vars=bind_vars)
        try:
            async for d in cur:
                await processor(converter(self._correct_process_doc_in_place(d)))
        finally:
            await cur.close(ignore_missing=True)

    async def add_deleted_match(self, match: models.DeletedMatch):
        """
        Adds a match in the deleted state to the database. This will overwrite any deleted match
        already present with the same internal match ID. Does not alter the source match.
        """
        await self._insert_model(
            match, match.internal_match_id, names.COLL_SRV_MATCHES_DELETED, overwrite=True)

    def _to_deleted_match(self, doc: dict[str, Any]) -> models.DeletedMatch:
        return models.DeletedMatch.construct(
            **models.remove_non_model_fields(doc, models.DeletedMatch))

    async def get_deleted_match(self, internal_match_id: str) -> models.DeletedMatch:
        """
        Get a match in the deleted state from the database given its internal match ID.
        """
        doc = await self._get_doc(
            names.COLL_SRV_MATCHES_DELETED, internal_match_id, errors.NoSuchMatchError)
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
        return await self._remove_subset(
            internal_match_id, last_access, names.COLL_SRV_MATCHES_DELETED)
    
    async def process_deleted_matches(
        self,
        processor: Callable[[models.DeletedMatch], Awaitable[None]]
    ):
        """
        Process deleted matches.

        processor - an async callable to which each match will be provided in turn.
        """
        await self._process_deleted_subset(
            processor, names.COLL_SRV_MATCHES_DELETED, self._to_deleted_match)

    async def _process_deleted_subset(
        self,
        processor: Callable[[models.DeletedMatch], Awaitable[None]],
        coll: str,
        converter: Callable[[dict[str, Any]], models.DeletedMatch | models.DeletedSelection]
    ):
        col = self._db.collection(coll)
        cur = await col.all()
        try:
            async for d in cur:
                await processor(converter(self._correct_process_doc_in_place(d)))
        finally:
            await cur.close(ignore_missing=True)

    def _data_product_process_key(self, dpid: models.DataProductProcessIdentifier) -> str:
        return f"{dpid.data_product}_{dpid.type.value}_{dpid.internal_id}"

    async def create_or_get_data_product_process(self, dp_match: models.DataProductProcess
    ) -> tuple[models.DataProductProcess, bool]:
        """
        Save the data product process to the database if it doesn't already exist, or
        get the current process if it does.

        Returns a tuple of the process as it currently exists in the database and a boolean
        indicating whether it was created or already existed.
        """
        key = self._data_product_process_key(models.DataProductProcessIdentifier(
            internal_id=dp_match.internal_id,
            data_product=dp_match.data_product,
            type=dp_match.type
        ))
        doc = jsonable_encoder(dp_match)
        doc[names.FLD_ARANGO_KEY] = key
        # See Note 1 at the beginning of the file
        col = self._db.collection(names.COLL_SRV_DATA_PRODUCT_PROCESSES)
        try:
            await col.insert(doc)
            return dp_match, False
        except DocumentInsertError as e:
            if e.error_code == _ARANGO_ERR_UNIQUE_CONSTRAINT:
                # Could possibly improve bandwidth by not getting missing_ids key,
                # would need to use AQL vs get()
                doc = await col.get({names.FLD_ARANGO_KEY: key})
                if not doc:
                    # This is highly unlikely. Not worth spending any time trying to recover
                    raise ValueError(
                        "Well, I tried. Either something is very wrong with the "
                        + "database or I just got really unlucky with timing on a process "
                        + "deletion. Try starting the process again.")
                self._correct_process_doc_in_place(doc)
                doc[models.FIELD_PROCESS_TYPE] = models.SubsetType(doc[models.FIELD_PROCESS_TYPE])
                return models.DataProductProcess.construct(
                    **models.remove_non_model_fields(doc, models.DataProductProcess)), True
            raise e

    async def send_data_product_heartbeat(
        self,
        dpid: models.DataProductProcessIdentifier,
        heartbeat_timestamp: int
    ):
        """
        Send a heartbeat to a data product process, updating the heartbeat timestamp.

        dpid - the data process ID.
        heartbeat_timestamp - the timestamp of the heartbeat in epoch milliseconts.
        """
        key = self._data_product_process_key(dpid)
        await self._send_heartbeat(
            names.COLL_SRV_DATA_PRODUCT_PROCESSES, key, heartbeat_timestamp, _ERRMAP[dpid.type])

    async def update_data_product_process_state(
        self,
        dpid: models.DataProductProcessIdentifier,
        state: models.ProcessState,
        update_time: int,
        missing_ids: list[str] | None = None,
    ):
        """
        Update the state of a data product process.

        dpid - the data process ID.
        state - the state to set
        update_time - the time at which the state was updated in epoch milliseconds
        missing_ids - any match or selection IDs that were not found when processing the match
            or selection.
        """
        await self._update_state(
            self._data_product_process_key(dpid),
            state,
            update_time,
            names.COLL_SRV_DATA_PRODUCT_PROCESSES,
            _ERRMAP[dpid.type],
            data_list=missing_ids,
            data_list_field=models.FIELD_DATA_PRODUCT_PROCESS_MISSING_IDS,
        )

    async def remove_data_product_process(self, dpid: models.DataProductProcessIdentifier):
        """
        Remove a data product process document.

        dpid - the data process ID.
        """
        key = self._data_product_process_key(dpid)
        col = self._db.collection(names.COLL_SRV_DATA_PRODUCT_PROCESSES)
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

    def _to_selection(self, doc: dict[str, Any]) -> models.Selection:
        return models.Selection.construct(
                **models.remove_non_model_fields(doc, models.Selection)
        )

    def _to_internal_selection(self, doc: dict[str, Any]) -> models.InternalSelection:
        return models.InternalSelection.construct(
                **models.remove_non_model_fields(doc, models.InternalSelection)
        )

    async def save_selection(self, selection: models.InternalSelection
    ) -> tuple[models.Selection, bool]:
        """
        Save a collection selection. If the selection already exists (based on the selection ID),
        that selection is updated with last access data from the incoming selection and
        the updated selection is returned.

        Returns a tuple of the selection and boolean indicating whether the selection already
        existed (true) or was created anew (false)
        """
        doc = jsonable_encoder(selection)
        doc[names.FLD_ARANGO_KEY] = selection.selection_id
        # See Note 1 at the beginning of the file
        col = self._db.collection(names.COLL_SRV_SELECTIONS)
        try:
            await col.insert(doc)
            return self._to_selection(doc), False
        except DocumentInsertError as e:
            if e.error_code == _ARANGO_ERR_UNIQUE_CONSTRAINT:
                try:
                    doc = await self.update_selection_last_access(
                        selection.selection_id, selection.last_access)
                    return self._to_selection(doc), True
                except errors.NoSuchSelectionError as e:
                    # This is highly unlikely. Not worth spending any time trying to recover
                    raise ValueError(
                        "Well, I tried. Either something is very wrong with the "
                        + "database or I just got really unlucky with timing on a selection "
                        + "deletion. Try matching again."
                    ) from e
            else:
                raise e

    async def remove_selection(self, selection_id: str, last_access: int) -> bool:
        """
        Removes the selection record from the database if the last access time is as provided.
        If the last access time does not match, it does nothing. This allows the selection to be
        removed safely after some reasonable period after a last access without a race condition,
        as a new access will change the access time and prevent the selection from being removed.

        Does not move the selection to a deleted state or otherwise modify the database.
        Deleting selections that have not completed running is not prevented, but is generally
        unwise.

        selection_id - the ID of the selection to remove.
        last_access - the time the selection was accessed last.

        Returns true if the selection document was removed, false otherwise.
        """
        return await self._remove_subset(selection_id, last_access, names.COLL_SRV_SELECTIONS)

    async def get_selection_full(self, selection_id: str, exception: bool = True
    ) -> models.InternalSelection:
        """
        Get the full selection associated with the selection id.

        selection_id - the ID of the selection.
        exception - True to throw an exception if the match is missing, False to return None.
        """
        doc = await self._get_doc(
            names.COLL_SRV_SELECTIONS,
            selection_id,
            errors.NoSuchSelectionError,
            exception=exception
        )
        return None if not doc else self._to_internal_selection(doc)

    async def get_selection_by_internal_id(self, internal_selection_id: str, exception: bool = True
    ) -> models.InternalSelection | None:
        """
        Get a selection by its internal ID.

        internal_selection_id - the internal ID of the selection.
        exception - throw an exception if the selection doesn't exist
        """
        doc = await self._get_subset_by_internal_id(
            names.COLL_SRV_SELECTIONS,
            internal_selection_id,
            models.FIELD_SELECTION_INTERNAL_SELECTION_ID,
            errors.NoSuchSelectionError,
            exception=exception,
        )
        return self._to_internal_selection(doc) if doc else None

    async def send_selection_heartbeat(self, internal_selection_id: str, heartbeat_timestamp: int):
        """
        Send a heartbeat to a selection, updating the heartbeat timestamp.

        internal_selection_id - the internal ID of the selection to modify.
        heartbeat_timestamp - the timestamp of the heartbeat in epoch milliseconds.
        """
        await self._send_heartbeat(
            names.COLL_SRV_SELECTIONS,
            internal_selection_id,
            heartbeat_timestamp,
            errors.NoSuchSelectionError,
            filter_key=models.FIELD_SELECTION_INTERNAL_SELECTION_ID,
        )

    async def update_selection_state(
        self,
        internal_selection_id: str,
        state: models.ProcessState,
        update_time: int,
        missing_selections: list[str] = None,
    ):
        """
        Update the state of the selection, optionally setting missing selection IDs.

        internal_selection_id - the internal ID of the selection to update
        state - the state of the selection to set
        update_time - the time at which the selection state was updated in epoch milliseconds
        missing_selections - the selection IDs to add to the missing attribute for the selection
        """
        await self._update_state(
            internal_selection_id,
            state,
            update_time,
            names.COLL_SRV_SELECTIONS,
            errors.NoSuchSelectionError,
            filter_key=models.FIELD_SELECTION_INTERNAL_SELECTION_ID,
            data_list=missing_selections,
            data_list_field=models.FIELD_SELECTION_UNMATCHED_IDS,
            data_count_field=models.FIELD_SELECTION_UNMATCHED_COUNT,
        )

    async def update_selection_last_access(self, selection_id, last_access):
        """
        Update the last time the active selection was accessed.
        Throws an error if the selection doesn't exist.

        selection_id - the ID for the selection.
        last_access - the time at which the selection was accessed in epoch milliseconds.
        """
        return await self._update_last_access(
            selection_id,
            names.COLL_SRV_SELECTIONS,
            last_access,
            errors.NoSuchSelectionError,
        )

    async def process_old_selections(
        self,
        selection_max_last_access_ms: int,
        processor: Callable[[models.InternalSelection], Awaitable[None]],
    ):
        """
        Process selections with a last access date older than the given date.

        selection_max_last_access_ms - process selections with a last access date older than this
            in epoch milliseconds.
        processor - an async callable to which each selection will be provided in turn.
        """
        await self._process_subsets(
            names.COLL_SRV_SELECTIONS,
            processor,
            self._to_internal_selection,
            max_last_access_ms=selection_max_last_access_ms,
        )
    
    async def process_collection_selections(
        self,
        collspec: models.CollectionSpec,
        processor: Callable[[models.InternalSelection], Awaitable[None]],
        states: set[models.ProcessState] | None = None,
    ):
        """
        Process selections in a collection.

        collspec - the collection to process and its version.
        processor - an async callable to which each selection will be provided in turn.
        states - filter by selection processing state (no filtering by default)
        """
        await self._process_subsets(
            names.COLL_SRV_SELECTIONS,
            processor,
            self._to_internal_selection,
            collspec=collspec,
            states=states,    
        )

    async def add_deleted_selection(self, selection: models.DeletedSelection):
        """
        Adds a selection in the deleted state to the database. This will overwrite any deleted
        selection already present with the same internal match ID. Does not alter the source
        selection.
        """
        await self._insert_model(
            selection,
            selection.internal_selection_id,
            names.COLL_SRV_SELECTIONS_DELETED,
            overwrite=True
        )

    def _to_deleted_selection(self, doc: dict[str, Any]) -> models.DeletedSelection:
        return models.DeletedSelection.construct(
            **models.remove_non_model_fields(doc, models.DeletedSelection))

    async def get_deleted_selection(self, internal_selection_id: str) -> models.DeletedSelection:
        """
        Get a selection in the deleted state from the database given its internal match ID.
        """
        doc = await self._get_doc(
            names.COLL_SRV_SELECTIONS_DELETED, internal_selection_id, errors.NoSuchSelectionError)
        return self._to_deleted_selection(doc)

    async def remove_deleted_selection(self, internal_selection_id: str, last_access: int) -> bool:
        """
        Removes the deleted selection record from the database if the last access time is as
        provided. If the last access time does not match, it does nothing. This allows the
        deleted selection to be removed safely without causing a race condition if another
        selection deletion thread updates the deleted selection record in between retrieving
        the selection from the database.

        Does not otherwise modify the database.

        internal_selection_id - the internal ID of the selection to remove.
        last_access - the time the selection was accessed last.

        Returns true if the selection document was removed, false otherwise.
        """
        return await self._remove_subset(
            internal_selection_id, last_access, names.COLL_SRV_SELECTIONS_DELETED)

    async def process_deleted_selections(
        self,
        processor: Callable[[models.DeletedMatch], Awaitable[None]]
    ):
        """
        Process deleted selections.

        processor - an async callable to which each selection will be provided in turn.
        """
        await self._process_deleted_subset(
            processor, names.COLL_SRV_SELECTIONS_DELETED, self._to_deleted_selection)

    async def _insert_model(
        self,
        model: BaseModel,
        key: str,
        collection: str,
        overwrite: bool = False
    ):
        doc = jsonable_encoder(model)
        doc[names.FLD_ARANGO_KEY] = key
        col = self._db.collection(collection)
        await col.insert(doc, overwrite=overwrite, silent=True)


    async def get_export_types(self, collection_id: str, data_product: str, load_ver: str
    ) -> list[str]:
        """
        Get the types available for export as sets to the Workspace.

        collection_id - the ID of the collection to check for export types.
        data_product - the data product to check for export types.
        load_ver - the load version of the data to check for export types.

        Returns an empty list if no type information could be found
        """
        aql = f"""
            FOR d IN @@{_FLD_COLLECTION}
                FILTER d.{names.FLD_COLLECTION_ID} == @{_FLD_COLLECTION_ID}
                FILTER d.{names.FLD_LOAD_VERSION} == @load_ver
                FILTER d.{names.FLD_DATA_PRODUCT} == @data_product
                RETURN d
            """
        bind_vars = {
            f"@{_FLD_COLLECTION}": names.COLL_EXPORT_TYPES,
            _FLD_COLLECTION_ID: collection_id,
            "load_ver": load_ver,
            "data_product": data_product
        }
        doc = await self._execute_aql_and_check_item_exists(
            aql, bind_vars, None, None, exception=False)
        return doc[names.FLD_TYPES] if doc else []
