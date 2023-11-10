"""
Module for working with analyzers for filtering via ArangoSearch.
"""
from src.service.storage_arango import ArangoStorage
from src.common.product_models.columnar_attribs_common_models import FilterStrategy
from src.common.storage.collection_and_field_names import COLLECTION_PREFIX

DEFAULT_ANALYZER = "identity"

_STRING_FULLTEXT_ANALYZER = "text_en"  # built in
_STRING_PREFIX_ANALYZER = f"{COLLECTION_PREFIX}text_en_prefix"
_STRING_NGRAM_ANALYZER = f"{COLLECTION_PREFIX}en_ngram3"
_STRING_NGRAM_ANALYZER_MIN_LEN = 3


_COL2ANALYZER = {
    FilterStrategy.FULL_TEXT: _STRING_FULLTEXT_ANALYZER,
    FilterStrategy.PREFIX: _STRING_PREFIX_ANALYZER,
    FilterStrategy.NGRAM: _STRING_NGRAM_ANALYZER,
}


_COL2MIN_LENGTH = {
    FilterStrategy.NGRAM: _STRING_NGRAM_ANALYZER_MIN_LEN
}


# TODO FILTERS what if we update the analyzers? Then we need to update any dependent views.
#      maybe the way to go is to update the analyzer name and make sure the view equivalency
#      check detects that 
_CUSTOM_ANALYZERS = {
    # For string prefix search
    # https://docs.arangodb.com/3.11/index-and-search/analyzers/#text
    _STRING_PREFIX_ANALYZER: (
        "text",
        {
            "locale": "en",
            "case": "lower",
            "accent": False,
            "stemming": False,
            "edgeNgram": {
                "min": 2,
                "max": 8,
                "preserveOriginal": True
            }
        },
        []
    ),
    _STRING_NGRAM_ANALYZER: (
        "pipeline",
        {"pipeline": [
            {
                "type": "norm",
                "properties": {
                    "locale": "en",
                    "case": "lower",
                    "accent": False
                }
            },
            {
                "type": "ngram",
                    "properties": {
                    "min": _STRING_NGRAM_ANALYZER_MIN_LEN,
                    "max": _STRING_NGRAM_ANALYZER_MIN_LEN,
                    "preserveOriginal": False,
                    "streamType": "utf8"
                 }
            }
        ]},
        ["position", "frequency"]
    ),
}


def get_analyzer(strategy: FilterStrategy | None, return_none_for_default_analyzer: bool = False
    ) -> str:
    """
    Get the name of the appropriate ArangoDB analyzer to use for a given filter strategy.
    
    strategy - the strategy to translate to an analyzer
    return_none_for_default_analyzer - rather than returning the default analyzer, return None.
    """
    df = None if return_none_for_default_analyzer else DEFAULT_ANALYZER
    return _COL2ANALYZER.get(strategy, df)


def get_minimum_query_length(strategy: FilterStrategy | None) -> int:
    """ Get the minimum allowable query length for a particular filter strategy. """
    return _COL2MIN_LENGTH.get(strategy, 0)


async def install_analyzers(db: ArangoStorage):
    """ Install required analyzers in the database. """
    for name, (type_, spec, features) in _CUSTOM_ANALYZERS.items():
        await db.create_analyzer(name, type_, spec, features)
