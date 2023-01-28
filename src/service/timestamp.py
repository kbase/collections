"""
Contains functions for creating timestamps.
"""

from datetime import datetime, timezone
import time


def timestamp():
    """ Creates a timestamp from the current time in ISO8601 format. """
    return datetime.now(timezone.utc).isoformat()


def now_epoch_millis():
    """ Returns the current time in Unix epoch milliseconds. """
    return int(time.time() * 1000)
