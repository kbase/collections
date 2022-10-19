"""
Exceptions thrown by the Collections system.
"""

# mostly copied from https://github.com/kbase/sample_service
# TODO add to some sort of package?

from enum import Enum
from typing import Optional


class ErrorType(Enum):
    """
    The type of an error, consisting of an error code and a brief string describing the type.
    :ivar error_code: an integer error code.
    :ivar error_type: a brief string describing the error type.
    """

    AUTHENTICATION_FAILED =  (10000, "Authentication failed")  # noqa: E222 @IgnorePep8
    """ A general authentication error. """

    NO_TOKEN =               (10010, "No authentication token")  # noqa: E222 @IgnorePep8
    """ No token was provided when required. """

    INVALID_TOKEN =          (10020, "Invalid token")  # noqa: E222 @IgnorePep8
    """ The token provided is not valid. """

    UNAUTHORIZED =           (20000, "Unauthorized")  # noqa: E222 @IgnorePep8
    """ The user is not authorized to perform the requested action. """

    MISSING_PARAMETER =      (30000, "Missing input parameter")  # noqa: E222 @IgnorePep8
    """ A required input parameter was not provided. """

    ILLEGAL_PARAMETER =      (30001, "Illegal input parameter")  # noqa: E222 @IgnorePep8
    """ An input parameter had an illegal value. """

    NO_SUCH_COLLECTION =     (40000, "No such collection")  # noqa: E222 @IgnorePep8
    """ The requested collection does not exist. """

    NO_SUCH_COLLECTION_VERSION = (40010, "No such collection version")  # noqa: E222 @IgnorePep8
    """ The requested collection version does not exist. """

    UNSUPPORTED_OP =         (100000, "Unsupported operation")  # noqa: E222 @IgnorePep8
    """ The requested operation is not supported. """

    def __init__(self, error_code, error_type):
        self.error_code = error_code
        self.error_type = error_type


class CollectionError(Exception):
    """
    The super class of all Collection related errors.
    :ivar error_type: the error type of this error.
    :ivar message: the message for this error.
    """

    def __init__(self, error_type: ErrorType, message: Optional[str] = None) -> None:
        '''
        Create a Collection error.
        :param error_type: the error type of this error.
        :param message: an error message.
        :raises TypeError: if error_type is None
        '''
        if not error_type:  # don't use not_falsy here, causes circular import
            raise TypeError('error_type cannot be None')
        msg = message.strip() if message and message.strip() else None
        super().__init__(msg)
        self.error_type = error_type
        self.message: Optional[str] = message


# leaving out a no token exception for now, I think FastAPI will deal with that.
# might need some custom error handling to have a standard error structure


class InvalidTokenError(CollectionError):
    """
    An error thrown when a user's token is invalid.
    """

    def __init__(self, message: str = None) -> None:
        super().__init__(ErrorType.INVALID_TOKEN, message)


class UnauthorizedError(CollectionError):
    """
    An error thrown when a user attempts a disallowed action.
    """

    def __init__(self, message: str = None) -> None:
        super().__init__(ErrorType.UNAUTHORIZED, message)


class MissingParameterError(CollectionError):
    """
    An error thrown when a required parameter is missing.
    """

    def __init__(self, message: str = None) -> None:
        super().__init__(ErrorType.MISSING_PARAMETER, message)


class IllegalParameterError(CollectionError):
    """
    An error thrown when a provided parameter is illegal.
    """

    def __init__(self, message: str = None) -> None:
        super().__init__(ErrorType.ILLEGAL_PARAMETER, message)


class NoDataException(CollectionError):
    """
    An error thrown when expected data does not exist.
    """

    def __init__(self, error_type: ErrorType, message: str) -> None:
        super().__init__(error_type, message)


class NoSuchCollectionError(NoDataException):
    """
    An error thrown when a collection does not exist.
    """

    def __init__(self, message: str) -> None:
        super().__init__(ErrorType.NO_SUCH_COLLECTION, message)


class NoSuchCollectionVersionError(NoDataException):
    """
    An error thrown when a collection version does not exist.
    """

    def __init__(self, message: str) -> None:
        super().__init__(ErrorType.NO_SUCH_COLLECTION_VERSION, message)
