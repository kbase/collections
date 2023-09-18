from src.common.product_models.columnar_attribs_common_models import FilterStrategy
from src.service.filtering import analyzers


# TODO add test for installing analyzers; mock DB. Will probably also need an integration test

def test_get_analyzer():
    test_cases = {
        FilterStrategy.FULL_TEXT: "text_en",
        FilterStrategy.PREFIX: "kbcoll_text_en_prefix",
        FilterStrategy.IDENTITY: "identity", 
        None: "identity"
    }
    for fs, expected in test_cases.items():
        assert analyzers.get_analyzer(fs) == expected

