import pytest
from unittest.mock import create_autospec, call

from src.service.storage_arango import ArangoStorage
from src.service import match_deletion
from src.service import models

# TODO TEST more tests. Right now just _delete_match tests because it's hard to test manually

MATCH = models.InternalMatch(
    match_id="foo",
    matcher_id="gtdb_lineage",
    collection_id="my_collection",
    collection_ver=3,
    user_parameters={},
    collection_parameters={},
    match_state=models.MatchState.COMPLETE,
    match_state_updated=80000,
    upas=[],
    matches=[],
    internal_match_id="internal_ID",
    wsids=[],
    created=60000,
    last_access=90000,
    user_last_perm_check={"user": 60000},
    deleted=100000,
)

DELETED = models.DeletedMatch(deleted=100000, **MATCH.dict())

MATCH_ALT_INTERNAL_ID = models.InternalMatch(
    **dict(MATCH.dict(), internal_match_id="internal_ID2"))

MATCH_NEWER_LAST_ACCESS = models.InternalMatch(**dict(MATCH.dict(), last_access=110000))

COLLECTION = models.SavedCollection(
    name="Some flowery name",
    ver_src="some_ver",
    data_products=[
        models.DataProduct(product="prodone", version="ver2"),
        models.DataProduct(product="prodtwo", version="ver6"),
    ],
    matchers=[],
    id="my_collection",
    ver_tag="some_tag",
    ver_num=3,
    date_create="2022-10-07T17:58:53.188698+00:00",
    user_create="some_user",
)


class DataProductSpecForAutoSpeccing:
    """
    Just exists to tell create_autospec the signature of the one required function.
    This function is not on the gereral DataProductSpec class, it's implemented only on 
    subclasses.

    Maybe should make an abstract class to inherit from or something...
    """
    async def delete_match(self, storage: ArangoStorage, internal_match_id: str): pass


# TODO TEST test logging for match deletion at some point, see
#      https://docs.pytest.org/en/7.1.x/how-to/logging.html


# Accessing hidden methods is poor practice, but trying to test _delete_match otherwise is very
# difficult. It might be wise to make it public?

@pytest.mark.asyncio
async def test_delete_match_standard_path():
    """
    Test the standard path through the _delete_match method where no undeleted, equivalent match
    remains in the DB.
    """
    # Test case where there's no equivalant undeleted match
    await _delete_match_standard_path(None)
    # Test case where there's a match with the same match ID but a different internal ID.
    # This means the match was deleted, but a new match with the same parameters was created and
    # therefore the match ID (which is the MD5 of the parameters) is the same. The internal
    # match ID is unique for every match instance, and so if the internal match ID is different
    # it's ok to delete the match data keyed to that ID.
    await _delete_match_standard_path(MATCH_ALT_INTERNAL_ID)


async def _delete_match_standard_path(return_match: models.InternalMatch):
    storage = create_autospec(ArangoStorage, spec_set=True, instance=True)
    data_product1 = create_autospec(DataProductSpecForAutoSpeccing, spec_set=True, instance=True)
    data_product2 = create_autospec(DataProductSpecForAutoSpeccing, spec_set=True, instance=True)
    dps = {"prodone": data_product1, "prodtwo": data_product2}

    storage.get_match_full.return_value = return_match
    storage.get_collection_version_by_num.return_value = COLLECTION

    await match_deletion._delete_match(storage, dps, DELETED)

    storage.get_match_full.assert_awaited_once_with("foo", exception=False)
    storage.remove_match.assert_not_awaited()
    storage.get_collection_version_by_num.assert_awaited_once_with("my_collection", 3)
    data_product1.delete_match.assert_awaited_once_with(storage, "internal_ID")
    data_product2.delete_match.assert_awaited_once_with(storage, "internal_ID")
    storage.remove_data_product_match.assert_has_awaits([
        call("internal_ID", "prodone"),
        call("internal_ID", "prodtwo"),
    ])
    assert storage.remove_data_product_match.await_count == 2
    storage.remove_deleted_match.assert_awaited_once_with("internal_ID", 90000)


@pytest.mark.asyncio
async def test_delete_match_delete_active_match():
    """
    Tests the case where a match is in the standard and deleted states, e.g. the match was
    moved to the deleted collection but not removed from the standard collection due to
    a server down or something, and deleting the standard match succeeds
    """
    storage = create_autospec(ArangoStorage, spec_set=True, instance=True)
    data_product1 = create_autospec(DataProductSpecForAutoSpeccing, spec_set=True, instance=True)
    data_product2 = create_autospec(DataProductSpecForAutoSpeccing, spec_set=True, instance=True)
    dps = {"prodone": data_product1, "prodtwo": data_product2}

    storage.get_match_full.return_value = MATCH
    storage.remove_match.return_value = True
    storage.get_collection_version_by_num.return_value = COLLECTION

    await match_deletion._delete_match(storage, dps, DELETED)

    storage.get_match_full.assert_awaited_once_with("foo", exception=False)
    storage.remove_match.assert_awaited_once_with("foo", 90000)
    storage.get_collection_version_by_num.assert_awaited_once_with("my_collection", 3)
    data_product1.delete_match.assert_awaited_once_with(storage, "internal_ID")
    data_product2.delete_match.assert_awaited_once_with(storage, "internal_ID")
    storage.remove_data_product_match.assert_has_awaits([
        call("internal_ID", "prodone"),
        call("internal_ID", "prodtwo"),
    ])
    assert storage.remove_data_product_match.await_count == 2
    storage.remove_deleted_match.assert_awaited_once_with("internal_ID", 90000)


@pytest.mark.asyncio
async def test_delete_match_delete_active_match_fail():
    """
    Tests the case where a match is in the standard and deleted states, e.g. the match was
    moved to the deleted collection but not removed from the standard collection due to
    a server down or something, and deleting the standard match fails.
    """
    storage = create_autospec(ArangoStorage, spec_set=True, instance=True)

    storage.get_match_full.return_value = MATCH
    storage.remove_match.return_value = False

    await match_deletion._delete_match(storage, {}, DELETED)

    storage.get_match_full.assert_awaited_once_with("foo", exception=False)
    storage.remove_match.assert_awaited_once_with("foo", 90000)
    storage.get_collection_version_by_num.assert_not_awaited()
    storage.remove_data_product_match.assert_not_awaited()
    storage.remove_deleted_match.assert_not_awaited()


@pytest.mark.asyncio
async def test_delete_match_delete_deleted_match_fail():
    """
    Tests the case where a match is in the standard and deleted states, e.g. the match was
    moved to the deleted collection but not removed from the standard collection due to
    a server down or something, and the last access times between the deleted match and the
    standard match differ, meaning the deleted match is out of date and should be removed
    from storage. Whether that is successful or not, the deletion routine punts and lets the
    next call of the deletion routine clean up.
    """
    storage = create_autospec(ArangoStorage, spec_set=True, instance=True)

    storage.get_match_full.return_value = MATCH_NEWER_LAST_ACCESS

    await match_deletion._delete_match(storage, {}, DELETED)

    storage.get_match_full.assert_awaited_once_with("foo", exception=False)
    storage.remove_match.assert_not_awaited()
    storage.remove_deleted_match.assert_awaited_once_with("internal_ID", 90000)
    storage.get_collection_version_by_num.assert_not_awaited()
    storage.remove_data_product_match.assert_not_awaited()
