"""
Global models for the service that cross API and storage boundaries. These models define
both

* The structure and keys accepted by and returned to the API
* The structure and keys accepted by and returned from the storage systems.

As such, changes must be made very carefully. For instance, a simple key change may mean
that older data in the database will no longer be fetched correctly - but automated tests
may still pass. In this case, a translation from the older data is required.
"""

from enum import Enum
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
FIELD_MATCH_LAST_ACCESS = "last_access"
FIELD_MATCH_USER_PERMS = "user_last_perm_check"
FIELD_MATCH_STATE = "match_state"
FIELD_MATCH_MATCHES = "matches"
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
FIELD_UPA_LIST_EXAMPLE = ["75/23/1", "100002/106/3;7/54/9"]
FIELD_UPA_LIST_DESCRIPTION= (
"""
The Unique Permanent Addresses (UPAs) for the input workspace objects
that are being matched against the collection. UPAs are in the format
W/O/V, where W is the workspace integer ID, O is the integer object ID, and
V is the version. The version may not be omitted. Reference paths
(e.g. a sequence of UPAs separated by ';') are allowed.
"""
)
FIELD_USER_PARAMETERS_EXAMPLE = {"rank": "genus"}
FIELD_USER_PARAMETERS_DESCRIPTION = "The user parameters for the match."


DATA_PRODUCT_ID_FIELD = Field(
    min_length = 1,
    max_length = 20,
    regex = "^[a-z_]+$",
    example="taxa_count",
    description="The ID of the data product"
)


# this seems stupid...
MATCHER_ID_PROPS = {
    "min_length": 1,
    "max_length": 20,
    "regex": "^[a-z_]+$",
    "example": "gtdb_lineage",
    "description": "The ID of the matcher",
}


MATCHER_ID_FIELD = Field(**MATCHER_ID_PROPS)


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


# https://fastapi.tiangolo.com//tutorial/path-params/#create-an-enum-class
class MatchState(str, Enum):
    """
    The state of a matching process.
    """
    PROCESSING = "processing"
    COMPLETE = "complete"
    FAILED = "failed"


class Match(BaseModel):
    """
    A match between KBase workspace service objects and data in a collection.
    """
    match_id: str = Field(
        description="The ID of the match; a unique but opaque string."
        # In practice, this ID is the MD5 of
        # * the matcher ID
        # * the collection ID and version
        #   * TODO MATCHERS invalidate the match if collection version doesn't match the
        #     current collection
        # * the match user parameters
        # * the input workspace service UPAs, sorted
    )
    matcher_id: str = Field(
        example="gtdb_lineage",
        description="The ID of the matcher performing the match."
    )
    collection_id: str = Field(
        example=FIELD_COLLECTION_ID_EXAMPLE,
        description="The ID of the collection for the match."
    )
    collection_ver: int = Field(
        example=7,
        description="The version of the collection for which the match was created."
    )
    user_parameters: dict[str, Any] = Field(
        example=FIELD_USER_PARAMETERS_EXAMPLE,
        description=FIELD_USER_PARAMETERS_DESCRIPTION,
    )
    match_state: MatchState = Field(
        example=MatchState.PROCESSING.value,
        description="The state of the matching process."
    )
    # TODO MATCHING add match state updated time stamp


class MatchVerbose(Match):
    """
    A match with the details of the match, e.g. input UPAs and resulting matching collection IDs.
    """
    # could add a UPA regex check here but that seems expensive and redundant. Will need to
    # parse the UPAs elsewhere anyway
    upas: list[str] = Field(
        example=FIELD_UPA_LIST_EXAMPLE,
        description=FIELD_UPA_LIST_DESCRIPTION,
    )
    matches: list[str] | None = Field(
        example=["GCA_000188315.1", "GCA_000172955.1"],
        description="Unique identifiers for the matches in the collection. The contents of the "
            + "strings will differ based on the matcher and the collection in question. "
            + "Null if the match is not yet complete."
    )  # This might get hairy. Hopefully we can lock this down a bit with more experience in
       # how this is going to work.
       # E.g. should it be the Arango _key value instead of the genome_id value in the
       # case of a gtdb_lineage matcher? The former is not useful to users, but is guaranteed
       # to be unique. However, the loaders should be guaranteeing uniqueness of the
       # genome_ids.
       # Do we need to be able to map the incoming UPAs to their matches? Potentially 1:M
       # Maybe this field should be internal only?


class InternalMatch(MatchVerbose):
    """
    Holds match fields for internal server use.
    """
    internal_match_id: str = Field(
        # bit of a long field name but probably not too many matches at one time
        example="e22f2d7d-7246-4636-a91b-13f29bc32d3d",
        description="An internal ID for the match that is unique per use. This allows for "
            + "deleting data for a match without the risk that a new match with the same "
            + "md5 ID is created and tries to read data in the process of deletion. "
            + "Expected to be a v4 UUID.",
    )
    wsids: set[int] = Field(
        examples={78, 10067},
        description="The set of workspace IDs from the UPAs."
    )
    # TODO MATCHERS heartbeat timestamp
    # We keep the created time internal since matches are not user specific. One user could
    # "create" a match but have it be really old. Avoid the confusion, keep it internal
    created: int = Field(
        example=1674243789864,
        description="Milliseconds since the Unix epoch at the point the match was created."
    )
    last_access: int = Field(
        example=1674243789864,
        description="Milliseconds since the Unix epoch at the point the match was last accessed. "
            + "Used for determining when to delete the match."
    )
    user_last_perm_check: dict[str, int] = Field(
        example={"user1": 1674243789451},
        description="A mapping of user name to the last time their permissions to view the "
            + "match was checked in Unix epoch milliseconds. Used to determine when to recheck "
            + "permissions for a user."
    )


def remove_non_model_fields(doc: dict, model: BaseModel) -> dict:
    """
    Removes any fields in `doc` that aren't fields in the pydantic model.
    """
    modelfields = set(model.__fields__.keys())
    return {f: doc[f] for f in doc if f in modelfields}
