"""
A registry of matchers available in the service.

Matchers must inherit from the Matcher class in src.service.matchers.common_models, as well
as implement the method:

    def generate_match_process(self,
        metadata: dict[str, dict[str, Any]],
        collection_parameters: dict[str, Any],
    ) -> CollectionProcess:
        
The method checks that input metadata allows for calculating the match and throws an exception
if that is not the case, and returns a CollectionProcess that can process the match.

metadata - the workspace metadata of the objects to match against, mapped by its UPA.
collection_parameters - the parameters for this match from the collection specification.
    It it expected that the parameters have been validated against the matcher schema
    for said parameters.
"""

# NOTE: Once a collection has been saved with a matcher, the matcher cannot be removed from the
# service without breaking that collection.

# very simple for now, just add your matcher here
# could add an actual registry method at some point, but seems like overkill for now

from src.service.matchers.common_models import Matcher
from src.service.matchers import lineage_matcher
from src.service.matchers import test_matcher

MATCHERS: dict[str, Matcher] = {
    lineage_matcher.MATCHER.id: lineage_matcher.MATCHER,
    test_matcher.MATCHER.id: test_matcher.MATCHER
}
