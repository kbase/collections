"""
Global models for the service that cross API and storage boundaries. These models define
both

* The structure and keys accepted by and returned to the API
* The structure and keys accepted by and returned from the storage systems.

As such, changes must be made very carefully. For instance, a simple key change may mean
that older data in the database will no longer be fetched correctly - but automated tests
may still pass. In this case, a translation from the older data is required.
"""

from pydantic import BaseModel, Field, validator
from typing import Optional

from src.service.arg_checkers import contains_control_characters

# TODO TEST all these regexes and constraints will need a good chunk of testing.

REGEX_NO_WHITESPACE = "^[^\\s]+$"

# regarding control character handling - you can check for control characters with the regex
# \pC escape - BUT Pydantic uses the built in re library which doesn't support it. The
# 3rd party regex library does, but that means you need to make a custom validator (like the
# ones below). If that's the case, might was well just use the control character checker
# we already have and which is pretty simple, and which can be easily customized to allow
# other control characters if needed (not likely).


# Model fields for use elsewhere
# If the model fields change, they must be changed here as well or things might break
# Is there a better way to do this? Seems absurdly hacky
FIELD_COLLECTION_ID = "id"
FIELD_VER_TAG = "ver_tag"
FIELD_VER_NUM = "ver_num"
FIELD_DATE_CREATE = "date_create"
FIELD_USER_CREATE = "user_create"


class DataProduct(BaseModel):
    """The ID and version of a data product associated with a collection"""
    product: str = Field(
        min_length = 1,
        max_length = 20,
        regex = r"^\w+$",
        example="taxa_freq",
        description="The ID of the data product"
    )
    version: str = Field(
        min_length = 1,
        max_length = 20,
        regex = r"^[\w.-]+$",
        example="gtdb.207.kbase.3",
        description="The load version of the data product"
    )
    # in the future we may want a schema version... need to think this through first.
    # assume missing == schema version 1 for now

    @validator("product", "version", pre=True)
    def _strip(cls, v):
        return v.strip()


class Collection(BaseModel):
    """
    A collection document provided to the collections service to save.
    """
    name: str = Field(
        min_length=1,
        max_length=50,
        example="Genome Taxonomy Database",
        description="The name of the collection."
    )
    ver_src: str = Field(
        min_length=1,
        max_length=50,
        regex=REGEX_NO_WHITESPACE,
        example="r207",
        description="The version of the collection at the collection data source."
    )
    desc: Optional[str] = Field(
        min_length=1,
        max_length=1000,
        example="This is a collection of used hot dogs collected from Coney Island in 1892.",
        description="A free text description of the collection."
    )
    data_products: list[DataProduct] = Field(
        description="The data products associated with the collection"
    )
    # TODO FIELDS icon url (and see about serving icons temporarily with the FAPI static server)

    @validator("name", "desc", pre=True)
    def _strip_and_fail_on_control_characters_with_exceptions(cls, v):
        if v is None:
            return None
        v = v.strip()
        pos = contains_control_characters(v, allow_tab_newline=True)
        if pos > -1: 
            raise ValueError(f"contains a non tab or newline control character at position {pos}")
        return v

    @validator("ver_src", pre=True)
    def _strip_and_fail_on_control_characters(cls, v):
        v = v.strip()
        pos = contains_control_characters(v)
        if pos > -1:
            raise ValueError(f"contains a control character at position {pos}")
        return v


# No need to worry about field validation here since the service is assigning the values
# Re the dates, since Arango doesn't have a special format for dates like mongo, we might as
# well store them as human readable strings. ISO8601 means they'll sort in correct order if
# necessary.
class SavedCollection(Collection):
    """
    A collection version returned from the collections service.
    """
    id: str = Field(
        example="GTDB",
        description="The unique ID of the collection."
    )
    ver_tag: str = Field(
        example="r207.kbase.2",
        description="A user assigned unique but otherwise arbitrary tag for the collection "
            + "version."
    )
    ver_num: int = Field(
        example=5,
        description="The numeric version of the collection, assigned by the collection service"
    )
    date_create: str = Field(
        example="2022-10-07T17:58:53.188698+00:00",
        description="The date the collection version was created in ISO8061 format"
    )
    user_create: str = Field(
        example="kbasehelp",
        description="The user that created the collection version."
    )


class ActiveCollection(SavedCollection):
    """
    An active collection version document returned from the collections service.
    """
    date_active: str = Field(
        example="2022-10-07T17:58:53.188698+00:00",
        description="The date the collection version was set as the active version in "
            + "ISO8601 format."
    )
    user_active: str = Field(
        example="kbasehelp",
        description="The user that activated the collection version."
    )
