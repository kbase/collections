"""
A registry of matchers available in the service.

Matchers must inherit from the Matcher class in src.service.matchers.common_models, as well
as implement the method:

    def generate_match_process(
        self,
        internal_match_id: str,
        metadata: dict[str, dict[str, Any]],
        user_parameters: dict[str, Any],
        collection_parameters: dict[str, Any],
        token: str,
    ) -> CollectionProcess:
        
The method checks that input metadata allows for calculating the match and throws an exception
if that is not the case, and returns a CollectionProcess that can process the match.

internal_match_id - the internal ID of the match.
metadata - the workspace metadata of the objects to match against, mapped by its UPA.
user_parameters - the parameters for this match supplied by the user. It it expected that the
    parameters have been validated against the matcher schema for said parameters.
collection_parameters - the parameters for this match from the collection specification.
    It it expected that the parameters have been validated against the matcher schema
    for said parameters.
token - the user's token.
"""
# TODO MATCHERS maybe the matcher should do the schema validation

# NOTE: Once a collection has been saved with a matcher, the matcher cannot be removed from the
# service without breaking that collection.

# very simple for now, just add your matcher here
# could add an actual registry method at some point, but seems like overkill for now

from src.service.matchers.common_models import Matcher
from src.service.matchers import (
    lineage_matcher,
    minhash_homology_matcher,
)

MATCHERS: dict[str, Matcher] = {
    lineage_matcher.MATCHER.id: lineage_matcher.MATCHER,
    minhash_homology_matcher.MATCHER.id: minhash_homology_matcher.MATCHER,
}
