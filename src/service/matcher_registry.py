"""
A registry of matchers available in the service.
"""

# very simple for now, just add your matcher here
# could add an actual registry method at some point, but seems like overkill for now

from src.service.matchers import lineage_matcher

_MATCHERS = [lineage_matcher.MATCHER]

MATCHERS = {}

for m in _MATCHERS:
    if m.id in MATCHERS:
        raise ValueError(f"matchers have the same id: {m.id}")
    MATCHERS[m.id] = m
