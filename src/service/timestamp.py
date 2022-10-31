"""
Contains a function for creating a timestamp.
"""

from datetime import datetime, timezone

def timestamp():
    """ Creates a timestamp from the current time in ISO8601 format. """
    return datetime.now(timezone.utc).isoformat()