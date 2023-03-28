import pytest
from unittest.mock import create_autospec, call

from src.service.storage_arango import ArangoStorage
from src.service import deletion
from src.service import models

# TODO TEST more tests. Right now just _delete_match tests because it's hard to test manually
#           Really should do some integration tests with an Arango connection, setting up
#           states where a match is in both the std and deleted state. Testing by bringing the
#           server up just winds up overwriting the match in the deleted state and none of the
#           checking code for that case is exercised


MATCH = models.InternalMatch(
    match_id="foo",
    matcher_id="gtdb_lineage",
    collection_id="my_collection",
    collection_ver=3,
    user_parameters={},
    collection_parameters={},
    state=models.ProcessState.COMPLETE,
    state_updated=80000,
    upas=[],
    matches=[],
    internal_match_id="internal_ID",
    wsids=[],
    created=60000,
    last_access=90000,
    user_last_perm_check={"user": 60000},
)

MATCH_DELETED = models.DeletedMatch(deleted=100000, **MATCH.dict())

MATCH_NEWER_LAST_ACCESS = models.InternalMatch(**dict(MATCH.dict(), last_access=110000))

SELECTION = models.InternalSelection(
    selection_id="whoo",
    collection_id="my_collection",
    collection_ver=3,
    data_product="fake",
    state=models.ProcessState.COMPLETE,
    state_updated=80000,
    selection_ids=[],
    internal_selection_id="selint1",
    created=60000,
    last_access=90000,
)

SELECTION_DELETED = models.DeletedSelection(deleted=100000, **SELECTION.dict())

SELECTION_NEWER_LAST_ACCESS = models.InternalSelection(**dict(SELECTION.dict(), last_access=110000))

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
    Just exists to tell create_autospec the signature of the required functions.
    These functions are not on the general DataProductSpec class; they're implemented only on 
    subclasses.

    Maybe should make an abstract class to inherit from or something...
    """
    async def delete_match(self, storage: ArangoStorage, internal_match_id: str): pass
    async def delete_selection(self, storage: ArangoStorage, internal_selection_id: str): pass


# TODO TEST test logging for match deletion at some point, see
#      https://docs.pytest.org/en/7.1.x/how-to/logging.html


# Accessing hidden methods is poor practice, but trying to test _delete_subset otherwise is very
# difficult. It might be wise to make it public?

@pytest.mark.asyncio
async def test_delete_match_standard_path():
    """
    Test the standard path through the _delete_subset method where no undeleted, equivalent match
    remains in the DB.
    """
    storage = create_autospec(ArangoStorage, spec_set=True, instance=True)
    data_product1 = create_autospec(DataProductSpecForAutoSpeccing, spec_set=True, instance=True)
    data_product2 = create_autospec(DataProductSpecForAutoSpeccing, spec_set=True, instance=True)
    dps = {"prodone": data_product1, "prodtwo": data_product2}
    dpid1 = models.DataProductProcessIdentifier(
        internal_id="internal_ID", data_product="prodone", type=models.SubsetType.MATCH
    )
    dpid2 = models.DataProductProcessIdentifier(
        internal_id="internal_ID", data_product="prodtwo", type=models.SubsetType.MATCH
    )

    storage.get_match_by_internal_id.return_value = None
    storage.get_collection_version_by_num.return_value = COLLECTION

    await deletion._delete_subset(storage, dps, MATCH_DELETED, models.SubsetType.MATCH)

    storage.get_match_by_internal_id.assert_awaited_once_with("internal_ID", exception=False)
    storage.remove_match.assert_not_awaited()
    storage.get_collection_version_by_num.assert_awaited_once_with("my_collection", 3)
    data_product1.delete_match.assert_awaited_once_with(storage, "internal_ID")
    data_product2.delete_match.assert_awaited_once_with(storage, "internal_ID")
    storage.remove_data_product_process.assert_has_awaits([call(dpid1), call(dpid2)])
    assert storage.remove_data_product_process.await_count == 2
    storage.remove_deleted_match.assert_awaited_once_with("internal_ID", 90000)


@pytest.mark.asyncio
async def test_delete_selection_standard_path():
    """
    Test the standard path through the _delete_subset method where no undeleted, equivalent
    selection remains in the DB.
    """
    storage = create_autospec(ArangoStorage, spec_set=True, instance=True)
    data_product1 = create_autospec(DataProductSpecForAutoSpeccing, spec_set=True, instance=True)
    data_product2 = create_autospec(DataProductSpecForAutoSpeccing, spec_set=True, instance=True)
    dps = {"prodone": data_product1, "prodtwo": data_product2}
    dpid1 = models.DataProductProcessIdentifier(
        internal_id="selint1", data_product="prodone", type=models.SubsetType.SELECTION
    )
    dpid2 = models.DataProductProcessIdentifier(
        internal_id="selint1", data_product="prodtwo", type=models.SubsetType.SELECTION
    )

    storage.get_selection_by_internal_id.return_value = None
    storage.get_collection_version_by_num.return_value = COLLECTION

    await deletion._delete_subset(storage, dps, SELECTION_DELETED, models.SubsetType.SELECTION)

    storage.get_selection_by_internal_id.assert_awaited_once_with("selint1", exception=False)
    storage.remove_selection.assert_not_awaited()
    storage.get_collection_version_by_num.assert_awaited_once_with("my_collection", 3)
    data_product1.delete_selection.assert_awaited_once_with(storage, "selint1")
    data_product2.delete_selection.assert_awaited_once_with(storage, "selint1")
    storage.remove_data_product_process.assert_has_awaits([call(dpid1), call(dpid2)])
    assert storage.remove_data_product_process.await_count == 2
    storage.remove_deleted_selection.assert_awaited_once_with("selint1", 90000)


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
    dpid1 = models.DataProductProcessIdentifier(
        internal_id="internal_ID", data_product="prodone", type=models.SubsetType.MATCH
    )
    dpid2 = models.DataProductProcessIdentifier(
        internal_id="internal_ID", data_product="prodtwo", type=models.SubsetType.MATCH
    )

    storage.get_match_by_internal_id.return_value = MATCH
    storage.remove_match.return_value = True
    storage.get_collection_version_by_num.return_value = COLLECTION

    await deletion._delete_subset(storage, dps, MATCH_DELETED, models.SubsetType.MATCH)

    storage.get_match_by_internal_id.assert_awaited_once_with("internal_ID", exception=False)
    storage.remove_match.assert_awaited_once_with("foo", 90000)
    storage.get_collection_version_by_num.assert_awaited_once_with("my_collection", 3)
    data_product1.delete_match.assert_awaited_once_with(storage, "internal_ID")
    data_product2.delete_match.assert_awaited_once_with(storage, "internal_ID")
    storage.remove_data_product_process.assert_has_awaits([call(dpid1), call(dpid2)])
    assert storage.remove_data_product_process.await_count == 2
    storage.remove_deleted_match.assert_awaited_once_with("internal_ID", 90000)


@pytest.mark.asyncio
async def test_delete_selection_delete_active_match():
    """
    Tests the case where a selection is in the standard and deleted states, e.g. the selection was
    moved to the deleted collection but not removed from the standard collection due to
    a server down or something, and deleting the standard selection succeeds
    """
    storage = create_autospec(ArangoStorage, spec_set=True, instance=True)
    data_product1 = create_autospec(DataProductSpecForAutoSpeccing, spec_set=True, instance=True)
    data_product2 = create_autospec(DataProductSpecForAutoSpeccing, spec_set=True, instance=True)
    dps = {"prodone": data_product1, "prodtwo": data_product2}
    dpid1 = models.DataProductProcessIdentifier(
        internal_id="selint1", data_product="prodone", type=models.SubsetType.SELECTION
    )
    dpid2 = models.DataProductProcessIdentifier(
        internal_id="selint1", data_product="prodtwo", type=models.SubsetType.SELECTION
    )

    storage.get_selection_by_internal_id.return_value = SELECTION
    storage.remove_selection.return_value = True
    storage.get_collection_version_by_num.return_value = COLLECTION

    await deletion._delete_subset(storage, dps, SELECTION_DELETED, models.SubsetType.SELECTION)

    storage.get_selection_by_internal_id.assert_awaited_once_with("selint1", exception=False)
    storage.remove_selection.assert_awaited_once_with("whoo", 90000)
    storage.get_collection_version_by_num.assert_awaited_once_with("my_collection", 3)
    data_product1.delete_selection.assert_awaited_once_with(storage, "selint1")
    data_product2.delete_selection.assert_awaited_once_with(storage, "selint1")
    storage.remove_data_product_process.assert_has_awaits([call(dpid1), call(dpid2)])
    assert storage.remove_data_product_process.await_count == 2
    storage.remove_deleted_selection.assert_awaited_once_with("selint1", 90000)


@pytest.mark.asyncio
async def test_delete_match_delete_active_match_fail():
    """
    Tests the case where a match is in the standard and deleted states, e.g. the match was
    moved to the deleted collection but not removed from the standard collection due to
    a server down or something, and deleting the standard match fails.
    """
    storage = create_autospec(ArangoStorage, spec_set=True, instance=True)

    storage.get_match_by_internal_id.return_value = MATCH
    storage.remove_match.return_value = False

    await deletion._delete_subset(storage, {}, MATCH_DELETED, models.SubsetType.MATCH)

    storage.get_match_by_internal_id.assert_awaited_once_with("internal_ID", exception=False)
    storage.remove_match.assert_awaited_once_with("foo", 90000)
    storage.get_collection_version_by_num.assert_not_awaited()
    storage.remove_data_product_process.assert_not_awaited()
    storage.remove_deleted_match.assert_not_awaited()


@pytest.mark.asyncio
async def test_delete_selection_delete_active_selection_fail():
    """
    Tests the case where a selection is in the standard and deleted states, e.g. the selection was
    moved to the deleted collection but not removed from the standard collection due to
    a server down or something, and deleting the standard selection fails.
    """
    storage = create_autospec(ArangoStorage, spec_set=True, instance=True)

    storage.get_selection_by_internal_id.return_value = SELECTION
    storage.remove_selection.return_value = False

    await deletion._delete_subset(storage, {}, SELECTION_DELETED, models.SubsetType.SELECTION)

    storage.get_selection_by_internal_id.assert_awaited_once_with("selint1", exception=False)
    storage.remove_selection.assert_awaited_once_with("whoo", 90000)
    storage.get_collection_version_by_num.assert_not_awaited()
    storage.remove_data_product_process.assert_not_awaited()
    storage.remove_deleted_selection.assert_not_awaited()


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

    storage.get_match_by_internal_id.return_value = MATCH_NEWER_LAST_ACCESS

    await deletion._delete_subset(storage, {}, MATCH_DELETED, models.SubsetType.MATCH)

    storage.get_match_by_internal_id.assert_awaited_once_with("internal_ID", exception=False)
    storage.remove_match.assert_not_awaited()
    storage.remove_deleted_match.assert_awaited_once_with("internal_ID", 90000)
    storage.get_collection_version_by_num.assert_not_awaited()
    storage.remove_data_product_process.assert_not_awaited()


@pytest.mark.asyncio
async def test_delete_selection_delete_deleted_selection_fail():
    """
    Tests the case where a selection is in the standard and deleted states, e.g. the selection was
    moved to the deleted collection but not removed from the standard collection due to
    a server down or something, and the last access times between the deleted selection and the
    standard selection differ, meaning the deleted selection is out of date and should be removed
    from storage. Whether that is successful or not, the deletion routine punts and lets the
    next call of the deletion routine clean up.
    """
    storage = create_autospec(ArangoStorage, spec_set=True, instance=True)

    storage.get_selection_by_internal_id.return_value = SELECTION_NEWER_LAST_ACCESS

    await deletion._delete_subset(storage, {}, SELECTION_DELETED, models.SubsetType.SELECTION)

    storage.get_selection_by_internal_id.assert_awaited_once_with("selint1", exception=False)
    storage.remove_selection.assert_not_awaited()
    storage.remove_deleted_selection.assert_awaited_once_with("selint1", 90000)
    storage.get_collection_version_by_num.assert_not_awaited()
    storage.remove_data_product_process.assert_not_awaited()
