"""
A registry of matchers available in the service.
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
