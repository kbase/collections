'''
A client for the KBase Auth2 server.
'''

# Mostly copied from https://github.com/kbase/sample_service with a few tweaks.
# TODO make a KBase auth library?

from enum import IntEnum


class AdminPermission(IntEnum):
    '''
    The different levels of admin permissions.
    '''
    NONE = 1
    # leave some space for potential future levels
    FULL = 10

