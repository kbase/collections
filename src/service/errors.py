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

    INVALID_AUTH_HEADER =    (10030, "Invalid authentication header")  # noqa: E222 @IgnorePep8
    """ The authentication header  is not valid. """

    UNAUTHORIZED =           (20000, "Unauthorized")  # noqa: E222 @IgnorePep8
    """ The user is not authorized to perform the requested action. """

    MISSING_PARAMETER =      (30000, "Missing input parameter")  # noqa: E222 @IgnorePep8
    """ A required input parameter was not provided. """

    ILLEGAL_PARAMETER =      (30001, "Illegal input parameter")  # noqa: E222 @IgnorePep8
    """ An input parameter had an illegal value. """

    REQUEST_VALIDATION_FAILED = (30010, "Request validation failed")  # noqa: E222 @IgnorePep8
    """ A request to a service failed validation of the request. """

    NO_REGISTERED_DATA_PRODUCT = (30020, "No registered data product")  # noqa: E222 @IgnorePep8
    """ There is no such data product registered with the collection. """

    NO_DATA_FOUND =          (40000, "Requested data not found")  # noqa: E222 @IgnorePep8
    """ The requested data does not exist. """
    
    NO_SUCH_COLLECTION =     (40010, "No such collection")  # noqa: E222 @IgnorePep8
    """ The requested collection does not exist. """

    NO_SUCH_COLLECTION_VERSION = (40020, "No such collection version")  # noqa: E222 @IgnorePep8
    """ The requested collection version does not exist. """

    COLLECTION_VERSION_EXISTS = (50000, "Collection version exists")  # noqa: E222 @IgnorePep8
    """ The requested collection version already exists. """

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

    def __init__(self, error_type: ErrorType, message: Optional[str] = None):
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


class AuthenticationError(CollectionError):
    """
    Super class for authentication related errors.
    """
    def __init__(self, error_type: ErrorType, message: Optional[str] = None):
        super().__init__(error_type, message)


class MissingTokenError(AuthenticationError):
    """
    An error thrown when a token is required but absent.
    """

    def __init__(self, message: str = None):
        super().__init__(ErrorType.NO_TOKEN, message)


class InvalidAuthHeader(AuthenticationError):
    """
    An error thrown when an authorization header is invalid.
    """

    def __init__(self, message: str = None):
        super().__init__(ErrorType.INVALID_AUTH_HEADER, message)


class InvalidTokenError(AuthenticationError):
    """
    An error thrown when a user's token is invalid.
    """

    def __init__(self, message: str = None):
        super().__init__(ErrorType.INVALID_TOKEN, message)


class UnauthorizedError(CollectionError):
    """
    An error thrown when a user attempts a disallowed action.
    """

    def __init__(self, message: str = None):
        super().__init__(ErrorType.UNAUTHORIZED, message)


class MissingParameterError(CollectionError):
    """
    An error thrown when a required parameter is missing.
    """

    def __init__(self, message: str = None):
        super().__init__(ErrorType.MISSING_PARAMETER, message)


class IllegalParameterError(CollectionError):
    """
    An error thrown when a provided parameter is illegal.
    """

    def __init__(self, message: str = None):
        super().__init__(ErrorType.ILLEGAL_PARAMETER, message)


class NoRegisteredDataProduct(CollectionError):
    """
    An error thrown when a requested data product is not registered with the collection.
    """

    def __init__(self, message: str = None):
        super().__init__(ErrorType.NO_REGISTERED_DATA_PRODUCT, message)


class NoDataException(CollectionError):
    """
    An error thrown when expected data does not exist.
    """

    def __init__(self, error_type: ErrorType, message: str):
        super().__init__(error_type, message)


class NoDataFoundError(NoDataException):
    """
    An generic error thrown when requested data does not exist. 
    """

    def __init__(self, message: str):
        super().__init__(ErrorType.NO_DATA_FOUND, message)


class NoSuchCollectionError(NoDataException):
    """
    An error thrown when a collection does not exist.
    """

    def __init__(self, message: str):
        super().__init__(ErrorType.NO_SUCH_COLLECTION, message)


class NoSuchCollectionVersionError(NoDataException):
    """
    An error thrown when a collection version does not exist.
    """

    def __init__(self, message: str):
        super().__init__(ErrorType.NO_SUCH_COLLECTION_VERSION, message)


class CollectionVersionExistsError(CollectionError):
    """
    An error thrown when a collection version already exists.
    """

    def __init__(self, message: str):
        super().__init__(ErrorType.COLLECTION_VERSION_EXISTS, message)
