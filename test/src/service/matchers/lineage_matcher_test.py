import pytest

import src.service.matchers.lineage_matcher as lineage_matcher


@pytest.mark.parametrize("version1, version2, expected_result", [
    ("214.0", "214.1", True),
    ("214", "214.11", True),
    ("214.8", "214.1.3", True),
    ("r214.0", "214.0", True),
    ("r214.0", "214.1", True),
    ("r214", "214", True),
    ("207.0", "214.1", False),
    ("r207.0", "214.1", False),
    ("foo", "bar", False),
    ("foo", "214.1", False),
])
def test_fuzzy_version_match(version1, version2, expected_result):
    assert lineage_matcher._fuzzy_version_match(version1, version2) == expected_result
