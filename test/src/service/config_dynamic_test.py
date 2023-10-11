from src.service.config_dynamic import DynamicConfigManager
from unittest.mock import create_autospec
from src.service.storage_arango import ArangoStorage


def test_noop():
    st = create_autospec(ArangoStorage, spect_set=True, instance=True)
    DynamicConfigManager(st)
