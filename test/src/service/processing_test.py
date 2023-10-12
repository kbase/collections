import re

from pytest import raises

from src.service.models import DataProductProcess, SubsetType, ProcessState
from src.service.processing import SubsetSpecification


def _create_dp_process(internal_id: str, state: ProcessState = ProcessState.COMPLETE
) -> DataProductProcess:
    return DataProductProcess(
        internal_id=internal_id,
        created=1,
        state_updated=1,
        data_product="foo",
        type=SubsetType.MATCH,
        state=state,
    )


def test_subset_specification_null_set():
    ss = SubsetSpecification()
    
    assert ss.internal_subset_id is None
    assert ss.mark_only is False
    assert ss.prefix is None
    assert ss.get_prefixed_subset_id() is None
    assert ss.get_subset_filtering_id() is None
    assert ss.is_null_subset() is True


def test_subset_specification_with_process():
    ss = SubsetSpecification(subset_process=_create_dp_process("my_proc_id"))
    
    assert ss.internal_subset_id == "my_proc_id"
    assert ss.mark_only is False
    assert ss.prefix is None
    assert ss.get_prefixed_subset_id() == "my_proc_id"
    assert ss.get_subset_filtering_id() == "my_proc_id"
    assert ss.is_null_subset() is False


def test_subset_specification_with_id_mark_and_prefix():
    ss = SubsetSpecification(internal_subset_id="proccyproc", mark_only=True, prefix="match_")
    
    assert ss.internal_subset_id == "proccyproc"
    assert ss.mark_only is True
    assert ss.prefix == "match_"
    assert ss.get_prefixed_subset_id() == "match_proccyproc"
    assert ss.get_subset_filtering_id() is None
    assert ss.is_null_subset() is False


def test_subset_specification_ignore_process():
    for s in set(ProcessState) - set([ProcessState.COMPLETE]):
        ss = SubsetSpecification(
            subset_process=_create_dp_process("boo", state=s),
            prefix="thing_"
        )
    
        assert ss.internal_subset_id is None
        assert ss.mark_only is False
        assert ss.prefix == "thing_"
        assert ss.get_prefixed_subset_id() is None
        assert ss.get_subset_filtering_id() is None
        assert ss.is_null_subset() is True


def test_subset_specification_fail_with_two_ids():
    expected = "Only one of internal_subset_id or subset_process may be provided"
    with raises(ValueError, match=f"^{re.escape(expected)}$"):
        SubsetSpecification(internal_subset_id="foo", subset_process=_create_dp_process("bar"))
