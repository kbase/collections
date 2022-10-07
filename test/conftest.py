
'''
Configure pytest fixtures and helper functions for this directory.
'''

import dateutil.parser
import time


def assert_close_to_now(iso8601timestamp: str):
    """
    Checks that an ISO8601 timestamp is within a second of the current time.
    """
    intime = dateutil.parser.parse(iso8601timestamp).timestamp()
    now_ms = time.time()
    assert now_ms + 1 > intime
    assert now_ms - 1 < intime
