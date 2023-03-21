"""
Functions for creating and hashing tokens.
"""

import hashlib
import secrets


def create_token(prefix=None):
    """
    Create a URL safe token using the default python bit size for secrets.

    prefix - an optional prefix to apply to the token string.
    """
    prefix = prefix if prefix else ""
    return "coll-selection-" + secrets.token_urlsafe()  # 256 bits by default


def hash_token(token: str):
    """
    SHA256 hash a token and return a hexdigest.
    """
    return hashlib.sha256(token.encode("utf-8")).hexdigest()
