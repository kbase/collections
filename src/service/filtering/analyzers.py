"""
Module for working with analyzers for filtering via ArangoSearch.
"""
from src.service.storage_arango import ArangoStorage
from src.common.product_models.columnar_attribs_common_models import FilterStrategy
from src.common.storage.collection_and_field_names import COLLECTION_PREFIX

DEFAULT_ANALYZER = "identity"

_STRING_FULLTEXT_ANALYZER = "text_en"  # built in

_STRING_PREFIX_ANALYZER = f"{COLLECTION_PREFIX}text_en_prefix"

_COL2ANALYZER = {
    FilterStrategy.FULL_TEXT: _STRING_FULLTEXT_ANALYZER,
    FilterStrategy.PREFIX: _STRING_PREFIX_ANALYZER,
}

_CUSTOM_ANALYZERS = {
    # For string prefix search
    # https://docs.arangodb.com/3.11/index-and-search/analyzers/#text
    _STRING_PREFIX_ANALYZER: (
        "text",
        {
            "locale": "en",
            "case": "lower",
            "accept": False,
            "stemming": False,
            "edgeNgram": {
                "min": 2,
                "max": 8,
                "preserveOriginal": True
            }
        },
        []
    )
}

def get_analyzer(strategy: FilterStrategy | None) -> str:
    """ Get the name of the appropriate ArangoDB analyzer to use for a given filter strategy. """
    return _COL2ANALYZER.get(strategy, DEFAULT_ANALYZER)


async def install_analyzers(db: ArangoStorage):
    """ Install required analyzers in the database. """
    for name, (type_, spec, features) in _CUSTOM_ANALYZERS.items():
        await db.create_analyzer(name, type_, spec, features)
