"""
Route constructs that are useful for the general service and data products, or across
multiple data products.
"""

from fastapi import Path

from src.service import errors
from src.service import models
from src.service.arg_checkers import contains_control_characters


PATH_VALIDATOR_COLLECTION_ID = Path(
    min_length=1,
    max_length=20,
    pattern=r"^\w+$",
    example=models.FIELD_COLLECTION_ID_EXAMPLE,
    description=models.FIELD_COLLECTION_ID_DESCRIPTION
)
""" A validator for collection ID path variables in route URIs. """


def err_on_control_chars(s: str, name: str):
    """
    Throw an error if a string contains control characters.

    s - the string to check
    name - the name of the string to include in error messages
    """
    pos = contains_control_characters(s)
    if pos > -1:
        raise errors.IllegalParameterError(
            f"{name} contains a control character at position {pos}")
