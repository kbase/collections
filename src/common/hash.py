"""
Functions to assist with hashing data.
"""

import hashlib


def md5_string(contents: str):
    """ Hashes a string input after encoding as utf-8 and returns the MD5 in hex"""
    return hashlib.md5(contents.encode('utf-8')).hexdigest()