"""
Global models for the service that cross API and storage boundaries. These models define
both

* The structure and keys accepted by and returned to the API
* The structure and keys accepted by and returned from the storage systems.

As such, changes must be made very carefully. For instance, a simple key change may mean
that older data in the database will no longer be fetched correctly - but automated tests
may still pass. In this case, a translation from the older data is required.
"""

from pydantic import BaseModel, Field, validator, HttpUrl
from typing import Any, Optional

from src.service.arg_checkers import contains_control_characters

# TODO TEST all these regexes and constraints will need a good chunk of testing.

REGEX_NO_WHITESPACE = "^[^\\s]+$"
REGEX_LOAD_VERSION = r"^[\w.-]+$"
LENGTH_MIN_LOAD_VERSION = 1
LENGTH_MAX_LOAD_VERSION = 20

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
FIELD_DATA_PRODUCTS = "data_products"
FIELD_DATA_PRODUCTS_PRODUCT = "product"
FIELD_MATCHERS = "matchers"
FIELD_MATCHERS_MATCHER = "matcher"
FIELD_DATE_CREATE = "date_create"
FIELD_USER_CREATE = "user_create"
FIELD_DATE_ACTIVE = "date_active"
FIELD_USER_ACTIVE = "user_active"

# Model metadata for use elsewhere
FIELD_COLLECTION_ID_EXAMPLE = "GTDB"
FIELD_COLLECTION_ID_DESCRIPTION = "The unique ID of the collection."
FIELD_VER_TAG_EXAMPLE = "r207.kbase.2"
FIELD_VER_TAG_DESCRIPTION = ("A user assigned unique but otherwise arbitrary tag for the "
    + "collection version.")
FIELD_VER_NUM_EXAMPLE = 5
FIELD_VER_NUM_DESCRIPTION = ("The numeric version of the collection, assigned by the "
    + "collection service")
FIELD_LOAD_VERSION_EXAMPLE = "gtdb.207.kbase.3"
FIELD_LOAD_VERSION_DESCRIPTION = "The load version of the data product"


DATA_PRODUCT_ID_FIELD = Field(
        min_length = 1,
        max_length = 20,
        regex = "^[a-z_]+$",
        example="taxa_count",
        description="The ID of the data product"
)


MATCHER_ID_FIELD = Field(
        min_length=1,
        max_length=20,
        regex="^[a-z_]+$",
        example="gtdb_lineage",
        description="The ID of the matcher",
)


class DataProduct(BaseModel):
    """The ID and version of a data product associated with a collection"""
    product: str = DATA_PRODUCT_ID_FIELD
    version: str = Field(
        min_length = LENGTH_MIN_LOAD_VERSION,
        max_length = LENGTH_MAX_LOAD_VERSION,
        regex = REGEX_LOAD_VERSION,
        example=FIELD_LOAD_VERSION_EXAMPLE,
        description=FIELD_LOAD_VERSION_DESCRIPTION
    )
    # in the future we may want a schema version... need to think this through first.
    # assume missing == schema version 1 for now

    @validator("product", "version", pre=True)
    def _strip(cls, v):
        return v.strip()


class Matcher(BaseModel):
    """The ID of a matcher associated with a collection and any parameters for the matcher"""
    matcher: str = MATCHER_ID_FIELD
    parameters: dict[str, Any] = Field(
        example={'gtdb_version': '207.0'},
        description="Any collection (as opposed to user provided) parameters for the matcher. "
            + "What these are will depend on the matcher in question"
    )


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
    icon_url: Optional[HttpUrl] = Field(
        example="https://live.staticflickr.com/3091/2883561418_dafc36c92b_z.jpg",
        description="A url to an image icon for the collection."
    )
    data_products: list[DataProduct] = Field(
        description="The data products associated with the collection"
    )
    matchers: list[Matcher] = Field(
        description="The matchers associated with the collection"
    )

    @validator("name", "ver_src", pre=True)
    def _strip_and_fail_on_control_characters(cls, v):
        v = v.strip()
        pos = contains_control_characters(v)
        if pos > -1:
            raise ValueError(f"contains a control character at position {pos}")
        return v

    @validator("desc", pre=True)
    def _strip_and_fail_on_control_characters_with_exceptions(cls, v):
        if v is None:
            return None
        v = v.strip()
        pos = contains_control_characters(v, allow_tab_newline=True)
        if pos > -1: 
            raise ValueError(f"contains a non tab or newline control character at position {pos}")
        return v

    @validator("data_products")
    def _check_data_products_unique(cls, v):
        return cls._checkunique(v, lambda dp: dp.product, "data product")

    @validator("matchers")
    def _check_matchers_unique(cls, v):
        return cls._checkunique(v, lambda m: m.matcher, "matcher")

    @classmethod
    def _checkunique(cls, items, accessor, field):
        seen = set()
        for it in items:
            if accessor(it) in seen:
                raise ValueError(f"duplicate {field}: {accessor(it)}")
            seen.add(accessor(it))
        return items

# No need to worry about field validation here since the service is assigning the values
# Re the dates, since Arango doesn't have a special format for dates like mongo, we might as
# well store them as human readable strings. ISO8601 means they'll sort in correct order if
# necessary.
class SavedCollection(Collection):
    """
    A collection version returned from the collections service.
    """
    id: str = Field(
        example=FIELD_COLLECTION_ID_EXAMPLE,
        description=FIELD_COLLECTION_ID_DESCRIPTION
    )
    ver_tag: str = Field(
        example=FIELD_VER_TAG_EXAMPLE,
        description=FIELD_VER_TAG_DESCRIPTION
    )
    ver_num: int = Field(
        example=FIELD_VER_NUM_EXAMPLE,
        description=FIELD_VER_NUM_DESCRIPTION
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
        example="2022-10-07T17:59:53.188698+00:00",
        description="The date the collection version was set as the active version in "
            + "ISO8601 format."
    )
    user_active: str = Field(
        example="otherkbasehelp",
        description="The user that activated the collection version."
    )
