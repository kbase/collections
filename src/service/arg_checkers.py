'''
Contains argument checkers for incoming data.
'''

# mostly copied from https://github.com/kbase/sample_service
# TODO add to some sort of package? Or maybe there's an existing package to replace this?

import datetime
import unicodedata
from typing import Optional, Iterable, TypeVar
from src.service.errors import IllegalParameterError, MissingParameterError

T = TypeVar('T')


def not_falsy(item: T, item_name: str) -> T:
    '''
    Check if a value is falsy and throw and exception if so.
    :param item: the item to check for falsiness.
    :param item_name: the name of the item to include in any exception.
    :raises ValueError: if the item is falsy.
    :returns: the item.
    '''
    if not item:
        raise ValueError(f'{item_name} cannot be a value that evaluates to false')
    return item


def not_falsy_in_iterable(
        iterable: Optional[Iterable[T]],
        name: str,
        allow_none: bool = False) -> Optional[Iterable[T]]:
    '''
    Check that an iterable is not None and contains no falsy items. Empty iterables are accepted.
    :param iterable: the iterable to check.
    :param name: the name of the iterable to be used in error messages.
    :param allow_none: allow the iterable to be None - in this case return None. The contents of
        the iterable may not be None.
    :returns: the iterable.
    :raises ValueError: if the iterable is None or contains falsy items.
    '''
    # probably need to allow for 0 as an option
    if iterable is None:
        if allow_none:
            return None
        raise ValueError(f'{name} cannot be None')
    for i, item in enumerate(iterable):
        not_falsy(item, f'Index {i} of iterable {name}')
    return iterable


def _contains_control_characters(string: str) -> bool:
    '''
    Check if a string contains control characters, as denoted by the Unicode character category
    starting with a C.
    :param string: the string to check.
    :returns: True if the string contains control characters, False otherwise.
    '''
    # make public if needed
    # See https://stackoverflow.com/questions/4324790/removing-control-characters-from-a-string-in-python  # noqa: E501
    for c in string:
        if unicodedata.category(c)[0] == 'C':
            return True
    return False


def _no_control_characters(string: str, name: str) -> str:
    '''
    Checks that a string contains no control characters and throws an exception if it does.
    See :meth:`contains_control_characters` for more information.
    :param string: The string to check.
    :param name: the name of the string to include in any exception.
    :raises IllegalParameterError: if the string contains control characters.
    :returns: the string.
    '''
    # make public if needed
    if _contains_control_characters(string):
        raise IllegalParameterError(name + ' contains control characters')
    return string


def check_string(string: Optional[str], name: str, max_len: int = None, optional: bool = False
                 ) -> Optional[str]:
    '''
    Check that a string meets a set of criteria:
    - it is not None or whitespace only (unless the optional parameter is specified)
    - it contains no control characters
    - (optional) it is less than some specified maximum length
    :param string: the string to test.
    :param name: the name of the string to be used in error messages.
    :param max_len: the maximum length of the string.
    :param optional: True if no error should be thrown if the string is None.
    :returns: the stripped string or None if the string was optional and None or whitespace only.
    :raises MissingParameterError: if the string is None or whitespace only.
    :raises IllegalParameterError: if the string is too long or contains illegal characters.
    '''
    # See the IDMapping service if character classes are needed.
    # Maybe package this stuff
    if max_len is not None and max_len < 1:
        raise ValueError('max_len must be > 0 if provided')
    if not string or not string.strip():
        if optional:
            return None
        raise MissingParameterError(name)
    string = string.strip()
    _no_control_characters(string, name)
    if max_len and len(string) > max_len:
        raise IllegalParameterError('{} exceeds maximum length of {}'.format(name, max_len))
    return string


def check_timestamp(timestamp: datetime.datetime, name: str):
    '''
    Check that a timestamp is not None and not naive. See
    https://docs.python.org/3.8/library/datetime.html#aware-and-naive-objects
    :param timestamp: the timestamp to check.
    :param name: the name of the variable to use in thrown errors.
    :returns: the timestamp.
    :raises ValueError: if the check fails.
    '''
    if not_falsy(timestamp, name).tzinfo is None:
        # The docs say you should also check savetime.tzinfo.utcoffset(savetime) is not None,
        # but initializing a datetime with a tzinfo subclass that returns None for that method
        # causes the constructor to throw an error
        raise ValueError(f'{name} cannot be a naive datetime')
    return timestamp